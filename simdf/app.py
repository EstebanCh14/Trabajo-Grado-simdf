import os
import sqlite3
import csv
import re
import unicodedata
import random
import warnings
import importlib
import importlib.util
from io import StringIO
from io import BytesIO
from datetime import datetime, timedelta
from functools import wraps
from urllib import error as urllib_error
from urllib import request as urllib_request

from flask import Flask, redirect, render_template, request, session, url_for, make_response, send_file, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

REPORTLAB_AVAILABLE = importlib.util.find_spec("reportlab") is not None

app = Flask(__name__)
app.secret_key = "simdf-secret-key"
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

DB_PATH = os.path.join(os.path.dirname(__file__), "simdf.db")
PROFILE_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "static", "uploads", "profiles")
PROFILE_ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
SIMDF_INTERNAL_TOKEN = os.environ.get("SIMDF_INTERNAL_TOKEN", "simdf-internal-key")
MODEL_PKL_CANDIDATES = (
    os.path.join(os.path.dirname(__file__), "modelo_fraude.pkl"),
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "modelo_fraude.pkl"),
)

MODEL_ARTIFACT = None
MODEL_ARTIFACT_LOADED = False
MODEL_ARTIFACT_ERROR = ""
MODEL_ARTIFACT_CATEGORIES = {}

TIPO_COMERCIO_LABELS = {
    "retail": "Retail",
    "restaurante": "Restaurante",
    "gasolinera": "Gasolinera",
    "ecommerce": "E-commerce",
    "transferencia": "Transferencia",
}

METODO_PAGO_LABELS = {
    "debito": "Débito",
    "credito": "Crédito",
    "transferencia": "Transferencia",
    "billetera": "Billetera digital",
}

UBICACION_LABELS = {
    "local": "Local",
    "nacional": "Nacional",
    "internacional": "Internacional",
}

MODEL_LABELS = {
    "random_forest": "Random Forest",
    "logistic_regression": "Logistic Regression",
    "lightgbm": "LightGBM",
}

LANGUAGE_LABELS = {
    "es": "Español",
    "en": "English",
}

THEME_LABELS = {
    "light": "Claro",
    "dark": "Oscuro",
}

INTERFACE_SIZE_LABELS = {
    "compact": "Compacto",
    "comfortable": "Confortable",
}

NOTIFICATION_FREQUENCY_LABELS = {
    "immediate": "Inmediata",
    "hourly": "Cada hora",
    "daily": "Resumen diario",
}

MODEL_CONFIG = {
    "random_forest": {"multiplier": 1.02, "bias": 6},
    "logistic_regression": {"multiplier": 0.95, "bias": -4},
    "lightgbm": {"multiplier": 1.08, "bias": 12},
}

MODEL_FEATURE_ORDER = ("monto", "tipo_comercio", "metodo_pago", "ubicacion", "frecuencia", "hora")

API_TRANSACTION_FIELDS = (
    "Genero",
    "Edad",
    "Ciudad",
    "Tipo_Cuenta",
    "Monto_USD",
    "Tipo_Transaccion",
    "Categoria_Comercio",
    "Balance_Cuenta_USD",
    "Dispositivo_Transaccion",
    "Tipo_Dispositivo",
    "Porcentaje_Gasto",
    "Transaccion_Grande",
    "Saldo_Restante",
    "Compra_Riesgosa",
    "Riesgo_Edad_Monto",
    "Dia_Semana",
    "Mes",
    "Hora",
    "Transaccion_Nocturna",
)

API_STRING_FIELDS = {
    "Genero",
    "Ciudad",
    "Tipo_Cuenta",
    "Tipo_Transaccion",
    "Categoria_Comercio",
    "Dispositivo_Transaccion",
    "Tipo_Dispositivo",
}

API_INTEGER_FIELDS = {
    "Edad",
    "Transaccion_Grande",
    "Compra_Riesgosa",
    "Dia_Semana",
    "Mes",
    "Hora",
    "Transaccion_Nocturna",
}

API_FLOAT_FIELDS = {
    "Monto_USD",
    "Balance_Cuenta_USD",
    "Porcentaje_Gasto",
    "Saldo_Restante",
    "Riesgo_Edad_Monto",
}

# In-memory backup for API ingestion flow (useful during local tests or transient DB issues).
API_TRANSACTIONS_MEMORY = []

SIMULATION_GEO_POINTS = {
    "local": [
        {"label": "Bogota, Colombia", "lat": 4.711, "lng": -74.0721},
        {"label": "Chapinero, Bogota, Colombia", "lat": 4.6486, "lng": -74.0637},
        {"label": "Usaquen, Bogota, Colombia", "lat": 4.7044, "lng": -74.0311},
    ],
    "nacional": [
        {"label": "Medellin, Colombia", "lat": 6.2442, "lng": -75.5812},
        {"label": "Cali, Colombia", "lat": 3.4516, "lng": -76.532},
        {"label": "Barranquilla, Colombia", "lat": 10.9685, "lng": -74.7813},
        {"label": "Cartagena, Colombia", "lat": 10.391, "lng": -75.4794},
    ],
    "internacional": [
        {"label": "Lima, Peru", "lat": -12.0464, "lng": -77.0428},
        {"label": "Ciudad de Mexico, Mexico", "lat": 19.4326, "lng": -99.1332},
        {"label": "Miami, Estados Unidos", "lat": 25.7617, "lng": -80.1918},
        {"label": "Madrid, Espana", "lat": 40.4168, "lng": -3.7038},
    ],
}

FIXED_MODEL_KEY = "lightgbm"
MAX_SIMULATION_BATCH = 200
FAILED_LOGIN_WINDOW_MINUTES = 15
FAILED_LOGIN_MAX_ATTEMPTS = 5
AUTO_BLOCK_MINUTES = 60

PAGE_META = {
    "login": {"page_title": "Login - SIMDF", "module_eyebrow": "ACCESO", "module_title": "Iniciar sesión"},
    "register": {"page_title": "Crear Cuenta - SIMDF", "module_eyebrow": "ACCESO", "module_title": "Crear cuenta"},
    "forgot_password": {"page_title": "Recuperar Contraseña - SIMDF", "module_eyebrow": "ACCESO", "module_title": "Recuperar contraseña"},
    "dashboard": {"page_title": "Dashboard - SIMDF", "module_eyebrow": "MÓDULO ACTIVO", "module_title": "Dashboard General"},
    "new_transaction": {"page_title": "Nueva Transacción - SIMDF", "module_eyebrow": "MÓDULO ACTIVO", "module_title": "Nueva Transacción"},
    "model_metrics": {"page_title": "Métricas del Modelo - SIMDF", "module_eyebrow": "MÓDULO ACTIVO", "module_title": "Métricas del Modelo"},
    "explainability": {"page_title": "Explicabilidad - SIMDF", "module_eyebrow": "MÓDULO ACTIVO", "module_title": "Explicabilidad del Modelo"},
    "history": {"page_title": "Historial - SIMDF", "module_eyebrow": "MÓDULO ACTIVO", "module_title": "Historial de Consultas"},
    "profile": {"page_title": "Mi Perfil - SIMDF", "module_eyebrow": "PERFIL", "module_title": "Mi perfil"},
    "analyst_config": {"page_title": "Configuración del Analista - SIMDF", "module_eyebrow": "PREFERENCIAS", "module_title": "Configuración del analista"},
    "analyst_notifications": {"page_title": "Notificaciones del Analista - SIMDF", "module_eyebrow": "NOTIFICACIONES", "module_title": "Configuración de notificaciones"},
    "analyst_security": {"page_title": "Seguridad del Analista - SIMDF", "module_eyebrow": "SEGURIDAD", "module_title": "Seguridad del analista"},
    "system_config": {"page_title": "Configuración - SIMDF", "module_eyebrow": "MÓDULO ACTIVO", "module_title": "Panel de configuración"},
    "system_logs": {"page_title": "Logs del Sistema - SIMDF", "module_eyebrow": "MÓDULO ACTIVO", "module_title": "Logs del sistema"},
    "system_security": {"page_title": "Seguridad del Sistema - SIMDF", "module_eyebrow": "ADMIN", "module_title": "Seguridad del sistema"},
    "user_management": {"page_title": "Gestión de Usuarios - SIMDF", "module_eyebrow": "ADMIN", "module_title": "Gestión de usuarios"},
    "model_management": {"page_title": "Gestión de Modelo - SIMDF", "module_eyebrow": "ADMIN", "module_title": "Gestión de modelo"},
    "data_management": {"page_title": "Gestión de Datos - SIMDF", "module_eyebrow": "ADMIN", "module_title": "Gestión de datos"},
    "result": {"page_title": "Resultado - SIMDF", "module_eyebrow": "RESULTADO", "module_title": "Resultado Inteligente"},
    "about": {"page_title": "Acerca de - SIMDF", "module_eyebrow": "INFORMACIÓN", "module_title": "Acerca de la aplicación"},
}


def get_model_catalog():
    return {
        "random_forest": {
            "accuracy": 91.4,
            "precision": 88.7,
            "recall": 89.9,
            "f1_score": 89.3,
            "auc_roc": 0.94,
            "roc_curve": [
                {"fpr": 0.0, "tpr": 0.0},
                {"fpr": 0.08, "tpr": 0.68},
                {"fpr": 0.16, "tpr": 0.83},
                {"fpr": 0.26, "tpr": 0.91},
                {"fpr": 1.0, "tpr": 1.0},
            ],
        },
        "logistic_regression": {
            "accuracy": 87.6,
            "precision": 84.1,
            "recall": 86.2,
            "f1_score": 85.1,
            "auc_roc": 0.89,
            "roc_curve": [
                {"fpr": 0.0, "tpr": 0.0},
                {"fpr": 0.12, "tpr": 0.59},
                {"fpr": 0.22, "tpr": 0.76},
                {"fpr": 0.34, "tpr": 0.87},
                {"fpr": 1.0, "tpr": 1.0},
            ],
        },
        "lightgbm": {
            "accuracy": 92.8,
            "precision": 90.6,
            "recall": 91.7,
            "f1_score": 91.1,
            "auc_roc": 0.96,
            "roc_curve": [
                {"fpr": 0.0, "tpr": 0.0},
                {"fpr": 0.06, "tpr": 0.72},
                {"fpr": 0.12, "tpr": 0.87},
                {"fpr": 0.22, "tpr": 0.94},
                {"fpr": 1.0, "tpr": 1.0},
            ],
        },
    }


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def format_db_datetime(db_datetime):
    try:
        parsed = datetime.strptime(db_datetime, "%Y-%m-%d %H:%M:%S")
        return parsed.strftime("%d/%m/%Y %H:%M")
    except (TypeError, ValueError):
        return db_datetime


def format_currency_usd(value):
    try:
        amount = float(value)
    except (TypeError, ValueError):
        amount = 0.0
    return f"USD {amount:,.2f}"


def normalize_csv_key(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", "_", text.strip().lower())
    return text.strip("_")


def find_csv_column(fieldnames, aliases):
    lookup = {}
    for name in fieldnames or []:
        normalized = normalize_csv_key(name)
        if normalized and normalized not in lookup:
            lookup[normalized] = name

    for alias in aliases:
        found = lookup.get(normalize_csv_key(alias))
        if found:
            return found
    return None


def map_dataset_tipo_comercio(raw_value):
    value = normalize_csv_key(raw_value)
    mapping = {
        "retail": "retail",
        "supermercado": "retail",
        "salud": "retail",
        "restaurante": "restaurante",
        "gasolinera": "gasolinera",
        "entretenimiento": "ecommerce",
        "ecommerce": "ecommerce",
        "e_commerce": "ecommerce",
        "transferencia": "transferencia",
    }
    return mapping.get(value, "retail")


def map_dataset_metodo_pago(raw_value):
    value = normalize_csv_key(raw_value)
    mapping = {
        "debito": "debito",
        "credito": "credito",
        "transferencia": "transferencia",
        "billetera": "billetera",
        "billetera_digital": "billetera",
        "pago_de_factura": "credito",
    }
    return mapping.get(value, "debito")


def map_dataset_ubicacion(raw_value):
    raw = str(raw_value or "").strip()
    normalized = normalize_csv_key(raw)

    if normalized in UBICACION_LABELS:
        return normalized, raw.lower()

    if normalized in {"local", "city", "ciudad"}:
        return "local", raw

    if normalized in {"nacional", "national", "pais"}:
        return "nacional", raw

    if normalized in {"internacional", "international", "exterior", "foreign"}:
        return "internacional", raw

    if raw:
        return "internacional", raw

    return "local", "local"


def parse_dataset_hour(raw_value):
    raw = str(raw_value or "").strip()
    if not raw:
        return 12.0

    if ":" in raw:
        raw = raw.split(":", 1)[0]

    raw = raw.replace(",", ".")
    hour = float(raw)
    return max(0.0, min(23.0, hour))


def get_current_decimal_hour(now_dt=None):
    current = now_dt or datetime.now()
    return round(current.hour + (current.minute / 60.0) + (current.second / 3600.0), 2)


def parse_hour_for_prediction(raw_value):
    raw = str(raw_value or "").strip().replace(",", ".")
    if not raw:
        return None

    if ":" in raw:
        try:
            hour_part, minute_part = raw.split(":", 1)
            hour = float(hour_part)
            minute = float(minute_part)
            return max(0.0, min(23.99, hour + (minute / 60.0)))
        except (TypeError, ValueError):
            return None

    try:
        hour = float(raw)
    except (TypeError, ValueError):
        return None

    return max(0.0, min(23.99, hour))


def parse_required_int(data, field_name, errors):
    raw_value = data.get(field_name)
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        errors.append(f"{field_name} debe ser un entero válido.")
        return None
    return value


def parse_required_float(data, field_name, errors):
    raw_value = data.get(field_name)
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        errors.append(f"{field_name} debe ser numérico.")
        return None
    return value


def validate_api_transaction_payload(payload):
    if not isinstance(payload, dict):
        return None, ["El cuerpo debe ser JSON válido."]

    errors = []
    missing_fields = [field for field in API_TRANSACTION_FIELDS if field not in payload]
    if missing_fields:
        errors.append(f"Faltan campos requeridos: {', '.join(missing_fields)}")
        return None, errors

    cleaned = {}

    for field in API_STRING_FIELDS:
        value = str(payload.get(field, "")).strip()
        if not value:
            errors.append(f"{field} no puede estar vacío.")
        cleaned[field] = value

    for field in API_INTEGER_FIELDS:
        cleaned[field] = parse_required_int(payload, field, errors)

    for field in API_FLOAT_FIELDS:
        cleaned[field] = parse_required_float(payload, field, errors)

    if cleaned.get("Edad") is not None and cleaned["Edad"] < 0:
        errors.append("Edad debe ser mayor o igual a 0.")
    if cleaned.get("Monto_USD") is not None and cleaned["Monto_USD"] < 0:
        errors.append("Monto_USD debe ser mayor o igual a 0.")
    if cleaned.get("Balance_Cuenta_USD") is not None and cleaned["Balance_Cuenta_USD"] < 0:
        errors.append("Balance_Cuenta_USD debe ser mayor o igual a 0.")
    if cleaned.get("Saldo_Restante") is not None and cleaned["Saldo_Restante"] < 0:
        errors.append("Saldo_Restante debe ser mayor o igual a 0.")
    if cleaned.get("Porcentaje_Gasto") is not None and cleaned["Porcentaje_Gasto"] < 0:
        errors.append("Porcentaje_Gasto debe ser mayor o igual a 0.")

    dia_semana = cleaned.get("Dia_Semana")
    if dia_semana is not None and not (1 <= dia_semana <= 7):
        errors.append("Dia_Semana debe estar entre 1 y 7.")

    mes = cleaned.get("Mes")
    if mes is not None and not (1 <= mes <= 12):
        errors.append("Mes debe estar entre 1 y 12.")

    hora = cleaned.get("Hora")
    if hora is not None and not (0 <= hora <= 23):
        errors.append("Hora debe estar entre 0 y 23.")

    for binary_field in ("Transaccion_Grande", "Compra_Riesgosa", "Transaccion_Nocturna"):
        value = cleaned.get(binary_field)
        if value is not None and value not in (0, 1):
            errors.append(f"{binary_field} debe ser 0 o 1.")

    return cleaned, errors


def predict_api_transaction_fraud(cleaned_payload):
    model = load_model_artifact()
    if model is None:
        return None, MODEL_ARTIFACT_ERROR or "Modelo no disponible"

    try:
        import pandas as pd  # pylint: disable=import-outside-toplevel

        model_input = {field: cleaned_payload[field] for field in API_TRANSACTION_FIELDS}
        X = pd.DataFrame([model_input])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            prediction = int(model.predict(X)[0])
        return prediction, ""
    except Exception as exc:
        return None, str(exc)


def compute_rule_based_risk_score(cleaned_payload):
    risk_score = 0
    if float(cleaned_payload["Monto_USD"]) > 1000:
        risk_score += 1
    if int(cleaned_payload["Transaccion_Nocturna"]) == 1:
        risk_score += 1
    if float(cleaned_payload["Porcentaje_Gasto"]) > 0.5:
        risk_score += 1
    return risk_score


def classify_risk_score(risk_score):
    if risk_score <= 0:
        return "segura"
    if risk_score == 1:
        return "sospechosa"
    return "fraude"


def build_api_transaction_record(record_id, cleaned_payload, model_prediction, risk_score, clasificacion, created_at):
    return {
        "id": record_id,
        "Genero": cleaned_payload["Genero"],
        "Edad": cleaned_payload["Edad"],
        "Ciudad": cleaned_payload["Ciudad"],
        "Tipo_Cuenta": cleaned_payload["Tipo_Cuenta"],
        "Monto_USD": cleaned_payload["Monto_USD"],
        "Tipo_Transaccion": cleaned_payload["Tipo_Transaccion"],
        "Categoria_Comercio": cleaned_payload["Categoria_Comercio"],
        "Balance_Cuenta_USD": cleaned_payload["Balance_Cuenta_USD"],
        "Dispositivo_Transaccion": cleaned_payload["Dispositivo_Transaccion"],
        "Tipo_Dispositivo": cleaned_payload["Tipo_Dispositivo"],
        "Porcentaje_Gasto": cleaned_payload["Porcentaje_Gasto"],
        "Transaccion_Grande": cleaned_payload["Transaccion_Grande"],
        "Saldo_Restante": cleaned_payload["Saldo_Restante"],
        "Compra_Riesgosa": cleaned_payload["Compra_Riesgosa"],
        "Riesgo_Edad_Monto": cleaned_payload["Riesgo_Edad_Monto"],
        "Dia_Semana": cleaned_payload["Dia_Semana"],
        "Mes": cleaned_payload["Mes"],
        "Hora": cleaned_payload["Hora"],
        "Transaccion_Nocturna": cleaned_payload["Transaccion_Nocturna"],
        "model_prediction": int(model_prediction),
        "risk_score": int(risk_score),
        "clasificacion": clasificacion,
        "created_at": created_at,
    }


def map_model_categoria_to_legacy_comercio(categoria_comercio):
    categoria = normalize_csv_key(categoria_comercio)
    mapping = {
        "supermercado": "retail",
        "restaurante": "restaurante",
        "salud": "retail",
        "electronica": "ecommerce",
        "entretenimiento": "gasolinera",
        "ropa": "retail",
    }
    return mapping.get(categoria, "retail")


def map_model_tipo_tx_to_legacy_metodo(tipo_transaccion):
    tipo_tx = normalize_csv_key(tipo_transaccion)
    mapping = {
        "debito": "debito",
        "credito": "credito",
        "transferencia": "transferencia",
        "pago_de_factura": "billetera",
        "retiro": "debito",
    }
    return mapping.get(tipo_tx, "debito")


def map_geo_label_to_legacy_ubicacion(geo_label):
    text = str(geo_label or "").strip().lower()
    if not text:
        return "local"

    if "colombia" not in text:
        return "internacional"

    if "bogota" in text or "bogotá" in text:
        return "local"

    return "nacional"


def get_geo_point_for_city(city_label):
    if not city_label:
        return None

    normalized_city = normalize_csv_key(city_label)
    for group in SIMULATION_GEO_POINTS.values():
        for point in group:
            if normalize_csv_key(point["label"]).split(",")[0] in normalized_city:
                return point

    if "bogota" in normalized_city or "bogotá" in normalized_city:
        return random.choice(SIMULATION_GEO_POINTS["local"])
    if "colombia" in normalized_city:
        return random.choice(SIMULATION_GEO_POINTS["nacional"])

    return random.choice(SIMULATION_GEO_POINTS["internacional"])


def resolve_geo_label_from_coordinates(lat, lng):
    try:
        lat_value = float(lat)
        lng_value = float(lng)
    except (TypeError, ValueError):
        return ""

    nearest_label = ""
    nearest_distance = None

    for points in SIMULATION_GEO_POINTS.values():
        for point in points:
            d_lat = lat_value - float(point["lat"])
            d_lng = lng_value - float(point["lng"])
            distance = (d_lat * d_lat) + (d_lng * d_lng)

            if nearest_distance is None or distance < nearest_distance:
                nearest_distance = distance
                nearest_label = point["label"]

    return nearest_label


def get_api_ingestion_user_id():
    conn = get_db_connection()
    row = conn.execute("SELECT id FROM users WHERE username = ? LIMIT 1", ("analista",)).fetchone()
    if not row:
        row = conn.execute("SELECT id FROM users WHERE role = ? LIMIT 1", ("analista",)).fetchone()
    if not row:
        row = conn.execute("SELECT id FROM users ORDER BY id LIMIT 1").fetchone()
    conn.close()
    return int(row["id"]) if row else 1


def build_consulta_from_api_payload(cleaned_payload, model_result, clasificacion, created_at):
    tipo_comercio = map_model_categoria_to_legacy_comercio(cleaned_payload["Categoria_Comercio"])
    metodo_pago = map_model_tipo_tx_to_legacy_metodo(cleaned_payload["Tipo_Transaccion"])
    ubicacion = map_geo_label_to_legacy_ubicacion(cleaned_payload["Ciudad"])
    hora = int(max(0, min(23, cleaned_payload.get("Hora", 0))))
    monto = round(float(cleaned_payload.get("Monto_USD", 0.0)), 2)

    porcentaje = float(cleaned_payload.get("Porcentaje_Gasto", 0.0) or 0.0)
    if porcentaje > 1:
        frecuencia = min(10.0, max(1.0, porcentaje / 10.0))
    else:
        frecuencia = min(10.0, max(1.0, porcentaje * 10.0))

    geo_point = get_geo_point_for_city(cleaned_payload.get("Ciudad"))
    if not geo_point:
        geo_group = SIMULATION_GEO_POINTS.get(ubicacion, SIMULATION_GEO_POINTS["local"])
        geo_point = random.choice(geo_group)

    resultado_text = {
        "segura": "No fraude",
        "sospechosa": "Sospechosa",
        "fraude": "Fraude",
    }.get(clasificacion, "Sin clasificación")

    return {
        "monto": monto,
        "tipo_comercio": tipo_comercio,
        "metodo_pago": metodo_pago,
        "ubicacion": ubicacion,
        "geo_latitude": geo_point["lat"],
        "geo_longitude": geo_point["lng"],
        "frecuencia": frecuencia,
        "hora": hora,
        "score": model_result.get("score", 0) if model_result else 0,
        "probabilidad_fraude": model_result.get("probabilidad_fraude", 0) if model_result else 0,
        "nivel_riesgo": model_result.get("nivel_riesgo", "Riesgo Medio") if model_result else "Riesgo Medio",
        "recomendacion": model_result.get("recomendacion", "Revisar manualmente") if model_result else "Revisar manualmente",
        "modelo": model_result.get("model_label", MODEL_LABELS.get(FIXED_MODEL_KEY, "LightGBM")) if model_result else MODEL_LABELS.get(FIXED_MODEL_KEY, "LightGBM"),
        "resultado": resultado_text,
        "created_at": created_at,
    }


def parse_fraud_label(raw_value):
    value = normalize_csv_key(raw_value)
    true_values = {
        "1",
        "true",
        "yes",
        "si",
        "fraude",
        "fraud",
        "fraudulent",
        "positivo",
        "positive",
        "alto",
        "high",
        "risk",
        "riesgo_alto",
    }
    false_values = {
        "0",
        "false",
        "no",
        "legit",
        "legitimate",
        "normal",
        "negativo",
        "negative",
        "bajo",
        "low",
        "non_fraud",
        "no_fraude",
    }

    if value in true_values:
        return 1
    if value in false_values:
        return 0
    return None


def is_valid_email(email):
    # Basic RFC-compliant structure validation for common corporate emails.
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    return bool(re.match(pattern, email or ""))


def validate_password_strength(password):
    if len(password or "") < 8:
        return False, "La contraseña debe tener al menos 8 caracteres."
    if not re.search(r"[A-Z]", password):
        return False, "La contraseña debe incluir al menos una letra mayúscula."
    if not re.search(r"[a-z]", password):
        return False, "La contraseña debe incluir al menos una letra minúscula."
    if not re.search(r"\d", password):
        return False, "La contraseña debe incluir al menos un número."
    if not re.search(r"[^A-Za-z0-9]", password):
        return False, "La contraseña debe incluir al menos un carácter especial."
    return True, ""


def is_allowed_profile_image(filename):
    if not filename or "." not in filename:
        return False
    extension = filename.rsplit(".", 1)[1].lower()
    return extension in PROFILE_ALLOWED_EXTENSIONS


def build_profile_image_filename(user_id, original_name):
    safe_name = secure_filename(original_name or "")
    extension = safe_name.rsplit(".", 1)[1].lower() if "." in safe_name else "jpg"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"user_{int(user_id)}_{timestamp}.{extension}"


def get_client_ip():
    forwarded_for = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    return forwarded_for or request.remote_addr or "unknown"


def get_client_device_label(user_agent):
    ua = (user_agent or "").lower()
    if "windows" in ua:
        return "Windows"
    if "mac os" in ua or "macintosh" in ua:
        return "macOS"
    if "android" in ua:
        return "Android"
    if "iphone" in ua or "ios" in ua:
        return "iPhone"
    if "chrome" in ua:
        return "Chrome"
    if "firefox" in ua:
        return "Firefox"
    return "Dispositivo desconocido"


def register_login_attempt(username, ip_address, success, failure_reason="", user_agent=""):
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO login_attempts (username, ip_address, success, failure_reason, user_agent)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            username or "",
            ip_address or "unknown",
            1 if success else 0,
            failure_reason or "",
            (user_agent or "")[:255],
        ),
    )
    conn.commit()
    conn.close()


def count_recent_failed_attempts(ip_address, minutes=FAILED_LOGIN_WINDOW_MINUTES):
    conn = get_db_connection()
    total = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM login_attempts
        WHERE ip_address = ?
          AND success = 0
          AND created_at >= datetime('now', ?)
        """,
        (ip_address, f"-{max(1, int(minutes))} minute"),
    ).fetchone()["total"]
    conn.close()
    return total or 0


def block_ip_address(ip_address, reason, minutes=AUTO_BLOCK_MINUTES, actor="sistema"):
    blocked_until = (datetime.now() + timedelta(minutes=max(5, int(minutes)))).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO blocked_ips (ip_address, reason, blocked_until, created_by)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ip_address)
        DO UPDATE SET
            reason = excluded.reason,
            blocked_until = excluded.blocked_until,
            created_by = excluded.created_by,
            created_at = CURRENT_TIMESTAMP
        """,
        (ip_address, reason, blocked_until, actor),
    )
    conn.commit()
    conn.close()
    return blocked_until


def get_active_ip_block(ip_address):
    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT ip_address, reason, blocked_until, created_by
        FROM blocked_ips
        WHERE ip_address = ?
          AND blocked_until > datetime('now')
        LIMIT 1
        """,
        (ip_address,),
    ).fetchone()
    conn.close()
    return row


def get_risk_badge_data(nivel_texto):
    nivel_lower = (nivel_texto or "").lower()
    if "alto" in nivel_lower:
        return "badge-alto", "🔴 Riesgo Alto"
    if "medio" in nivel_lower:
        return "badge-medio", "🟡 Riesgo Medio"
    if "bajo" in nivel_lower:
        return "badge-bajo", "🟢 Riesgo Bajo"
    return "badge-neutral", "⚪ Sin nivel"


def log_event(event_type, message, actor_username=None, severity="INFO"):
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO system_logs (event_type, severity, message, actor_username)
        VALUES (?, ?, ?, ?)
        """,
        (event_type, severity, message, actor_username or session.get("username", "sistema")),
    )
    conn.commit()
    conn.close()


def get_app_settings():
    defaults = {
        "active_model": FIXED_MODEL_KEY,
        "fraud_block_threshold": "4",
        "block_minutes": "30",
        "alert_email_enabled": "1",
        "alert_recipient": "fraude@simdf.local",
        "language": "es",
        "theme": "light",
        "interface_size": "compact",
    }

    conn = get_db_connection()
    rows = conn.execute("SELECT setting_key, setting_value FROM app_settings").fetchall()
    conn.close()

    settings = defaults.copy()
    for row in rows:
        settings[row["setting_key"]] = row["setting_value"]

    settings["active_model"] = FIXED_MODEL_KEY
    if settings.get("language") not in LANGUAGE_LABELS:
        settings["language"] = "es"
    if settings.get("theme") not in THEME_LABELS:
        settings["theme"] = "light"
    if settings.get("interface_size") not in INTERFACE_SIZE_LABELS:
        settings["interface_size"] = "compact"
    return settings


def save_app_settings(new_settings):
    conn = get_db_connection()
    for key, value in new_settings.items():
        conn.execute(
            """
            INSERT INTO app_settings (setting_key, setting_value)
            VALUES (?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value
            """,
            (key, str(value)),
        )
    conn.commit()
    conn.close()


def get_user_ui_preferences(user_id):
    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT theme_preference, interface_size
        FROM users
        WHERE id = ?
        """,
        (user_id,),
    ).fetchone()
    conn.close()

    theme = (row["theme_preference"] if row else "") or ""
    interface_size = (row["interface_size"] if row else "") or ""

    if theme not in THEME_LABELS:
        theme = ""
    if interface_size not in INTERFACE_SIZE_LABELS:
        interface_size = ""

    return {
        "theme": theme,
        "interface_size": interface_size,
    }


def resolve_model_artifact_path():
    for candidate in MODEL_PKL_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    return ""


def normalize_model_text(value):
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_text.strip().lower()


def select_known_category(column_name, preferred_value, default_value=""):
    categories = MODEL_ARTIFACT_CATEGORIES.get(column_name, [])
    if not categories:
        return preferred_value or default_value

    preferred = str(preferred_value or "")
    if preferred in categories:
        return preferred

    normalized_preferred = normalize_model_text(preferred)
    for option in categories:
        if normalize_model_text(option) == normalized_preferred:
            return option

    if default_value and default_value in categories:
        return default_value

    if default_value:
        normalized_default = normalize_model_text(default_value)
        for option in categories:
            if normalize_model_text(option) == normalized_default:
                return option

    return categories[0]


def apply_model_unpickle_compat():
    try:
        from sklearn.compose import _column_transformer as _ct  # pylint: disable=import-outside-toplevel

        if not hasattr(_ct, "_RemainderColsList"):
            class _RemainderColsList(list):
                pass

            _ct._RemainderColsList = _RemainderColsList
    except Exception:
        # If sklearn is unavailable, model loading will fail and fallback will handle scoring.
        return


def load_model_artifact():
    global MODEL_ARTIFACT, MODEL_ARTIFACT_LOADED, MODEL_ARTIFACT_ERROR, MODEL_ARTIFACT_CATEGORIES
    if MODEL_ARTIFACT_LOADED:
        return MODEL_ARTIFACT

    MODEL_ARTIFACT_LOADED = True
    model_path = resolve_model_artifact_path()
    if not model_path:
        MODEL_ARTIFACT_ERROR = "No se encontró modelo_fraude.pkl en el workspace."
        return None

    try:
        import joblib  # pylint: disable=import-outside-toplevel

        apply_model_unpickle_compat()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            MODEL_ARTIFACT = joblib.load(model_path)

        MODEL_ARTIFACT_CATEGORIES = {}
        preprocessor = None
        if hasattr(MODEL_ARTIFACT, "named_steps"):
            preprocessor = MODEL_ARTIFACT.named_steps.get("preprocesamiento")
        if preprocessor and hasattr(preprocessor, "named_transformers_"):
            cat_encoder = preprocessor.named_transformers_.get("cat")
            if cat_encoder is not None and hasattr(cat_encoder, "categories_"):
                for idx, column_name in enumerate(preprocessor.transformers_[0][2]):
                    MODEL_ARTIFACT_CATEGORIES[column_name] = [str(v) for v in cat_encoder.categories_[idx]]
    except Exception as exc:
        MODEL_ARTIFACT = None
        MODEL_ARTIFACT_ERROR = str(exc)

    return MODEL_ARTIFACT


def build_artifact_feature_row(monto, tipo_comercio, metodo_pago, ubicacion, frecuencia, hora, artifact_features=None):
    if artifact_features:
        hora_input = artifact_features.get("Hora", hora)
        hora_int = int(max(0, min(23, round(float(hora_input)))))

        feature_row = {
            "Genero": select_known_category("Genero", normalize_model_text(artifact_features.get("Genero")), "Masculino"),
            "Edad": int(max(0, round(float(artifact_features.get("Edad", 35))))),
            "Ciudad": select_known_category("Ciudad", normalize_model_text(artifact_features.get("Ciudad")), "Bangalore"),
            "Tipo_Cuenta": select_known_category("Tipo_Cuenta", normalize_model_text(artifact_features.get("Tipo_Cuenta")), "Ahorros"),
            "Monto_USD": round(float(artifact_features.get("Monto_USD", monto)), 4),
            "Tipo_Transaccion": select_known_category(
                "Tipo_Transaccion",
                normalize_model_text(artifact_features.get("Tipo_Transaccion")),
                "Transferencia",
            ),
            "Categoria_Comercio": select_known_category(
                "Categoria_Comercio",
                normalize_model_text(artifact_features.get("Categoria_Comercio")),
                "Supermercado",
            ),
            "Balance_Cuenta_USD": round(float(artifact_features.get("Balance_Cuenta_USD", max(float(monto) * 2.2, 450.0))), 4),
            "Dispositivo_Transaccion": select_known_category(
                "Dispositivo_Transaccion",
                normalize_model_text(artifact_features.get("Dispositivo_Transaccion")),
                "Computador",
            ),
            "Tipo_Dispositivo": select_known_category(
                "Tipo_Dispositivo",
                normalize_model_text(artifact_features.get("Tipo_Dispositivo")),
                "Computador",
            ),
            "Porcentaje_Gasto": round(float(artifact_features.get("Porcentaje_Gasto", 0.0)), 4),
            "Transaccion_Grande": int(max(0, round(float(artifact_features.get("Transaccion_Grande", 0))))),
            "Saldo_Restante": round(float(artifact_features.get("Saldo_Restante", 0.0)), 4),
            "Compra_Riesgosa": int(max(0, round(float(artifact_features.get("Compra_Riesgosa", 0))))),
            "Riesgo_Edad_Monto": round(float(artifact_features.get("Riesgo_Edad_Monto", 0.0)), 4),
            "Dia_Semana": int(max(1, min(7, round(float(artifact_features.get("Dia_Semana", datetime.now().isoweekday())))))),
            "Mes": int(max(1, min(12, round(float(artifact_features.get("Mes", datetime.now().month)))))),
            "Hora": hora_int,
            "Transaccion_Nocturna": int(max(0, round(float(artifact_features.get("Transaccion_Nocturna", 0))))),
        }
        return feature_row

    now_dt = datetime.now()
    hora_int = int(max(0, min(23, round(float(hora)))))

    tipo_tx_map = {
        "debito": "Débito",
        "credito": "Crédito",
        "transferencia": "Transferencia",
        "billetera": "Pago de Factura",
    }
    categoria_map = {
        "retail": "Supermercado",
        "restaurante": "Restaurante",
        "gasolinera": "Entretenimiento",
        "ecommerce": "Electrónica",
        "transferencia": "Electrónica",
    }
    ciudad_map = {
        "local": "Bangalore",
        "nacional": "Mumbai",
        "internacional": "Delhi",
    }

    edad = 35
    balance_cuenta = max(float(monto) * 2.2, 450.0)
    porcentaje_gasto = (float(monto) / balance_cuenta) * 100 if balance_cuenta > 0 else 0.0
    saldo_restante = max(balance_cuenta - float(monto), 0.0)
    transaccion_grande = 1 if float(monto) >= 900 else 0
    transaccion_nocturna = 1 if (hora_int <= 5 or hora_int >= 23) else 0
    compra_riesgosa = 1 if str(tipo_comercio) in {"ecommerce", "transferencia"} or str(ubicacion) == "internacional" else 0
    riesgo_edad_monto = round(float(monto) / max(1, edad), 4)

    feature_row = {
        "Genero": select_known_category("Genero", "Masculino", "Masculino"),
        "Edad": edad,
        "Ciudad": select_known_category("Ciudad", ciudad_map.get(str(ubicacion), "Bangalore"), "Bangalore"),
        "Tipo_Cuenta": select_known_category("Tipo_Cuenta", "Ahorros", "Ahorros"),
        "Monto_USD": round(float(monto), 4),
        "Tipo_Transaccion": select_known_category(
            "Tipo_Transaccion",
            tipo_tx_map.get(str(metodo_pago), "Transferencia"),
            "Transferencia",
        ),
        "Categoria_Comercio": select_known_category(
            "Categoria_Comercio",
            categoria_map.get(str(tipo_comercio), "Supermercado"),
            "Supermercado",
        ),
        "Balance_Cuenta_USD": round(balance_cuenta, 4),
        "Dispositivo_Transaccion": select_known_category("Dispositivo_Transaccion", "Computador", "Computador"),
        "Tipo_Dispositivo": select_known_category("Tipo_Dispositivo", "Computador", "Computador"),
        "Porcentaje_Gasto": round(porcentaje_gasto, 4),
        "Transaccion_Grande": transaccion_grande,
        "Saldo_Restante": round(saldo_restante, 4),
        "Compra_Riesgosa": compra_riesgosa,
        "Riesgo_Edad_Monto": riesgo_edad_monto,
        "Dia_Semana": int(now_dt.isoweekday()),
        "Mes": int(now_dt.month),
        "Hora": hora_int,
        "Transaccion_Nocturna": transaccion_nocturna,
    }

    return feature_row


def evaluate_risk_with_artifact(monto, tipo_comercio, metodo_pago, ubicacion, frecuencia, hora, model_key, artifact_features=None):
    model = load_model_artifact()
    if model is None:
        return None

    try:
        import pandas as pd  # pylint: disable=import-outside-toplevel

        row = build_artifact_feature_row(monto, tipo_comercio, metodo_pago, ubicacion, frecuencia, hora, artifact_features)
        X = pd.DataFrame([row])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw_pred = int(model.predict(X)[0])

            if hasattr(model, "predict_proba"):
                classes = list(getattr(model, "classes_", [0, 1]))
                proba_row = model.predict_proba(X)[0]
                fraud_index = classes.index(1) if 1 in classes else max(0, len(classes) - 1)
                fraud_prob = float(proba_row[fraud_index])
            else:
                fraud_prob = 1.0 if raw_pred == 1 else 0.0

        probabilidad_fraude = max(1, min(99, int(round(fraud_prob * 100))))
        score = round(fraud_prob * 1000, 2)

        if raw_pred == 1 or probabilidad_fraude >= 70:
            nivel_riesgo = "Riesgo Alto"
            recomendacion = "Bloquear"
            clase = "alto"
        elif probabilidad_fraude >= 40:
            nivel_riesgo = "Riesgo Medio"
            recomendacion = "Revisar manualmente"
            clase = "medio"
        else:
            nivel_riesgo = "Riesgo Bajo"
            recomendacion = "Aprobar"
            clase = "bajo"

        return {
            "model_key": model_key,
            "model_label": MODEL_LABELS.get(model_key, "LightGBM"),
            "score": score,
            "probabilidad_fraude": probabilidad_fraude,
            "nivel_riesgo": nivel_riesgo,
            "recomendacion": recomendacion,
            "clase": clase,
        }
    except Exception as exc:
        global MODEL_ARTIFACT_ERROR
        MODEL_ARTIFACT_ERROR = str(exc)
        return None


def evaluate_risk_by_model(monto, tipo_comercio, metodo_pago, ubicacion, frecuencia, hora, model_key, artifact_features=None):
    artifact_result = evaluate_risk_with_artifact(
        monto,
        tipo_comercio,
        metodo_pago,
        ubicacion,
        frecuencia,
        hora,
        model_key,
        artifact_features,
    )
    if artifact_result:
        return artifact_result

    comercio_riesgo = {
        "retail": 35,
        "restaurante": 20,
        "gasolinera": 30,
        "ecommerce": 60,
        "transferencia": 75,
    }

    metodo_riesgo = {
        "debito": 15,
        "credito": 25,
        "transferencia": 45,
        "billetera": 35,
    }

    ubicacion_riesgo = {
        "local": 10,
        "nacional": 30,
        "internacional": 65,
    }

    hora_riesgo = 50 if hora < 6 or hora > 22 else 15

    base_score = (
        monto * 0.35
        + frecuencia * 18
        + comercio_riesgo.get(tipo_comercio, 25)
        + metodo_riesgo.get(metodo_pago, 20)
        + ubicacion_riesgo.get(ubicacion, 20)
        + hora_riesgo
    )

    cfg = MODEL_CONFIG.get(model_key, MODEL_CONFIG["random_forest"])
    score = (base_score * cfg["multiplier"]) + cfg["bias"]
    probabilidad_fraude = max(1, min(99, round((score / 600) * 100)))

    if probabilidad_fraude >= 70:
        nivel_riesgo = "Riesgo Alto"
        recomendacion = "Bloquear"
        clase = "alto"
    elif probabilidad_fraude >= 40:
        nivel_riesgo = "Riesgo Medio"
        recomendacion = "Revisar manualmente"
        clase = "medio"
    else:
        nivel_riesgo = "Riesgo Bajo"
        recomendacion = "Aprobar"
        clase = "bajo"

    return {
        "model_key": model_key,
        "model_label": MODEL_LABELS.get(model_key, "Random Forest"),
        "score": round(score, 2),
        "probabilidad_fraude": probabilidad_fraude,
        "nivel_riesgo": nivel_riesgo,
        "recomendacion": recomendacion,
        "clase": clase,
    }


def build_model_feature_values(transaction_data):
    return (
        float(transaction_data["monto"]),
        str(transaction_data["tipo_comercio"]),
        str(transaction_data["metodo_pago"]),
        str(transaction_data["ubicacion"]),
        float(transaction_data["frecuencia"]),
        float(transaction_data["hora"]),
    )


def generate_random_transaction():
    current_hour = get_current_decimal_hour()
    generated_type = random.choices(
        ["buena", "regular", "riesgosa"],
        weights=[45, 35, 20],
        k=1,
    )[0]

    if generated_type == "buena":
        tx = {
            "monto": round(random.uniform(8.0, 140.0), 2),
            "tipo_comercio": random.choice(["retail", "restaurante", "gasolinera"]),
            "metodo_pago": random.choice(["debito", "credito"]),
            "ubicacion": "local",
            "frecuencia": round(random.uniform(0.2, 2.2), 2),
            "hora": current_hour,
        }
    elif generated_type == "regular":
        tx = {
            "monto": round(random.uniform(70.0, 420.0), 2),
            "tipo_comercio": random.choice(["retail", "ecommerce", "gasolinera", "restaurante"]),
            "metodo_pago": random.choice(["credito", "debito", "billetera"]),
            "ubicacion": random.choice(["local", "nacional"]),
            "frecuencia": round(random.uniform(1.5, 5.5), 2),
            "hora": current_hour,
        }
    else:
        tx = {
            "monto": round(random.uniform(280.0, 1500.0), 2),
            "tipo_comercio": random.choice(["transferencia", "ecommerce"]),
            "metodo_pago": random.choice(["transferencia", "billetera", "credito"]),
            "ubicacion": random.choice(["nacional", "internacional"]),
            "frecuencia": round(random.uniform(4.0, 10.5), 2),
            "hora": current_hour,
        }

    return tx, generated_type


def generate_random_geo_location(ubicacion_key):
    location_key = str(ubicacion_key or "local").strip().lower()
    available_points = SIMULATION_GEO_POINTS.get(location_key)
    if not available_points:
        available_points = (
            SIMULATION_GEO_POINTS["local"]
            + SIMULATION_GEO_POINTS["nacional"]
            + SIMULATION_GEO_POINTS["internacional"]
        )

    base_point = random.choice(available_points)

    # Small jitter to avoid identical coordinates while keeping city-level coherence.
    lat = round(float(base_point["lat"]) + random.uniform(-0.018, 0.018), 6)
    lng = round(float(base_point["lng"]) + random.uniform(-0.018, 0.018), 6)

    return {
        "geo_location_label": base_point["label"],
        "geo_latitude": lat,
        "geo_longitude": lng,
    }


def parse_simulation_count(raw_value, default=1, min_value=1, max_value=MAX_SIMULATION_BATCH):
    try:
        count = int(raw_value)
    except (TypeError, ValueError):
        count = default

    if count < min_value:
        count = min_value
    if count > max_value:
        count = max_value
    return count


def store_consulta_record(
    conn,
    user_id,
    monto,
    tipo_comercio,
    metodo_pago,
    ubicacion,
    frecuencia,
    hora,
    score,
    probabilidad_fraude,
    nivel_riesgo,
    recomendacion,
    selected_model,
    geo_latitude=None,
    geo_longitude=None,
):
    icono_riesgo = {"alto": "🔴", "medio": "🟡", "bajo": "🟢"}.get(nivel_riesgo_to_class(nivel_riesgo), "⚪")
    resultado = f"{icono_riesgo} {nivel_riesgo} · {recomendacion}"

    conn.execute(
        """
        INSERT INTO consultas (
            user_id, monto, tipo_comercio, metodo_pago, ubicacion, geo_latitude, geo_longitude, frecuencia, hora,
            score, probabilidad_fraude, nivel_riesgo, recomendacion, resultado
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            monto,
            tipo_comercio,
            metodo_pago,
            ubicacion,
            geo_latitude,
            geo_longitude,
            frecuencia,
            hora,
            score,
            probabilidad_fraude,
            nivel_riesgo,
            recomendacion,
            resultado,
        ),
    )
    conn.execute(
        "UPDATE consultas SET modelo = ? WHERE id = last_insert_rowid()",
        (selected_model,),
    )

    return resultado


def nivel_riesgo_to_class(nivel_riesgo):
    valor = str(nivel_riesgo or "").lower()
    if "alto" in valor:
        return "alto"
    if "medio" in valor:
        return "medio"
    return "bajo"


def safe_pct(numerator, denominator):
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def compute_auc_and_curve(scores, labels):
    if not scores or not labels or len(scores) != len(labels):
        return 0.5, [{"fpr": 0.0, "tpr": 0.0}, {"fpr": 1.0, "tpr": 1.0}]

    total_pos = sum(1 for y in labels if int(y) == 1)
    total_neg = len(labels) - total_pos
    if total_pos == 0 or total_neg == 0:
        return 0.5, [{"fpr": 0.0, "tpr": 0.0}, {"fpr": 1.0, "tpr": 1.0}]

    thresholds = [i / 100 for i in range(0, 101, 5)]
    points = []
    for threshold in thresholds:
        tp = fp = tn = fn = 0
        for score, label in zip(scores, labels):
            predicted = 1 if float(score) >= threshold else 0
            actual = 1 if int(label) == 1 else 0
            if predicted == 1 and actual == 1:
                tp += 1
            elif predicted == 1 and actual == 0:
                fp += 1
            elif predicted == 0 and actual == 0:
                tn += 1
            else:
                fn += 1

        tpr = (tp / (tp + fn)) if (tp + fn) else 0.0
        fpr = (fp / (fp + tn)) if (fp + tn) else 0.0
        points.append({"fpr": round(fpr, 4), "tpr": round(tpr, 4)})

    points.sort(key=lambda p: p["fpr"])
    if points[0]["fpr"] != 0.0 or points[0]["tpr"] != 0.0:
        points.insert(0, {"fpr": 0.0, "tpr": 0.0})
    if points[-1]["fpr"] != 1.0 or points[-1]["tpr"] != 1.0:
        points.append({"fpr": 1.0, "tpr": 1.0})

    auc = 0.0
    for idx in range(1, len(points)):
        x1, y1 = points[idx - 1]["fpr"], points[idx - 1]["tpr"]
        x2, y2 = points[idx]["fpr"], points[idx]["tpr"]
        auc += (x2 - x1) * ((y1 + y2) / 2)

    auc = max(0.0, min(1.0, auc))
    return round(auc, 3), points


def get_dynamic_model_metrics(model_key, user_id=None):
    conn = get_db_connection()
    where_clauses = ["modelo = ?", "true_fraud_label IN (0, 1)"]
    params = [model_key]
    if user_id is not None:
        where_clauses.append("user_id = ?")
        params.append(user_id)

    rows = conn.execute(
        f"""
        SELECT probabilidad_fraude, true_fraud_label
        FROM consultas
        WHERE {' AND '.join(where_clauses)}
        """,
        tuple(params),
    ).fetchall()
    inferred_mode = False

    if not rows:
        # Backward compatibility for datasets montados antes de guardar etiqueta real.
        legacy_where = ["modelo = ?"]
        legacy_params = [model_key]
        if user_id is not None:
            legacy_where.append("user_id = ?")
            legacy_params.append(user_id)

        legacy_rows = conn.execute(
            f"""
            SELECT probabilidad_fraude, frecuencia
            FROM consultas
            WHERE {' AND '.join(legacy_where)}
            """,
            tuple(legacy_params),
        ).fetchall()
        conn.close()

        if not legacy_rows:
            return None

        inferred_rows = []
        valid_legacy = 0
        for row in legacy_rows:
            try:
                freq = float(row["frecuencia"])
            except (TypeError, ValueError):
                continue

            if abs(freq - 3.0) < 0.001 or abs(freq - 9.0) < 0.001:
                inferred_label = 1 if freq >= 8 else 0
                inferred_rows.append(
                    {
                        "probabilidad_fraude": row["probabilidad_fraude"],
                        "true_fraud_label": inferred_label,
                    }
                )
                valid_legacy += 1

        if valid_legacy == 0:
            return None

        rows = inferred_rows
        inferred_mode = True
    else:
        conn.close()

    tp = fp = tn = fn = 0
    scores = []
    labels = []

    for row in rows:
        label = int(row["true_fraud_label"])
        score_pct = int(row["probabilidad_fraude"] or 0)
        score_ratio = max(0.0, min(1.0, score_pct / 100.0))
        pred = 1 if score_pct >= 70 else 0

        labels.append(label)
        scores.append(score_ratio)

        if pred == 1 and label == 1:
            tp += 1
        elif pred == 1 and label == 0:
            fp += 1
        elif pred == 0 and label == 0:
            tn += 1
        else:
            fn += 1

    total = tp + tn + fp + fn
    accuracy = safe_pct(tp + tn, total)
    precision = safe_pct(tp, tp + fp)
    recall = safe_pct(tp, tp + fn)
    f1 = 0.0
    if precision + recall > 0:
        f1 = round((2 * precision * recall) / (precision + recall), 2)

    auc_roc, roc_curve = compute_auc_and_curve(scores, labels)

    return {
        "metrics": {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "auc_roc": auc_roc,
            "roc_curve": roc_curve,
        },
        "confusion": {
            "tn": tn,
            "fp": fp,
            "fn": fn,
            "tp": tp,
        },
        "labeled_count": total,
        "inferred_mode": inferred_mode,
    }


def has_mounted_dataset_uploads():
    conn = get_db_connection()
    total = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM dataset_uploads
        WHERE target_table = 'consultas'
          AND records_imported > 0
        """
    ).fetchone()["total"]
    conn.close()
    return int(total or 0) > 0


def maybe_block_user_after_frauds(user_id, username):
    settings = get_app_settings()
    threshold = max(2, int(settings.get("fraud_block_threshold", "4")))
    block_minutes = max(5, int(settings.get("block_minutes", "30")))

    conn = get_db_connection()
    high_frauds = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM consultas
        WHERE user_id = ?
          AND probabilidad_fraude >= 70
          AND created_at >= datetime('now', '-1 day')
        """,
        (user_id,),
    ).fetchone()["total"]

    if high_frauds >= threshold:
        blocked_until = (datetime.now() + timedelta(minutes=block_minutes)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE users SET blocked_until = ? WHERE id = ?",
            (blocked_until, user_id),
        )
        conn.commit()
        conn.close()
        log_event(
            "SECURITY",
            f"Usuario {username} bloqueado temporalmente por {high_frauds} fraudes altos en 24h.",
            actor_username="sistema",
            severity="WARN",
        )
        return blocked_until

    conn.close()
    return None


def escape_pdf_text(value):
    return str(value).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_simple_pdf(title, lines):
    visible_lines = list(lines[:52])

    content_parts = [
        "BT",
        "/F1 14 Tf",
        "50 812 Td",
        "(SIMDF) Tj",
        "0 -14 Td",
        "/F1 9 Tf",
        "(Sistema Interno de Monitoreo y Deteccion de Fraude) Tj",
        "0 -18 Td",
        "/F1 11 Tf",
        f"({escape_pdf_text(title)}) Tj",
        "0 -18 Td",
        "/F1 8 Tf",
    ]

    for line in visible_lines:
        content_parts.append(f"({escape_pdf_text(line)}) Tj")
        content_parts.append("0 -12 Td")

    content_parts.append("ET")

    content_parts.extend(
        [
            "BT",
            "/F1 8 Tf",
            "50 44 Td",
            "(SIMDF - Sistema de deteccion de fraude) Tj",
            "0 -11 Td",
            "(Reporte generado automaticamente) Tj",
            "0 -11 Td",
            "(Universidad XXXXX - 2026) Tj",
            "ET",
        ]
    )

    content_stream = "\n".join(content_parts).encode("latin-1", "replace")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Count 1 /Kids [3 0 R] >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(content_stream)).encode("ascii") + b" >>\nstream\n" + content_stream + b"\nendstream",
    ]

    pdf = b"%PDF-1.4\n"
    offsets = [0]

    for idx, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf += f"{idx} 0 obj\n".encode("ascii")
        pdf += obj
        pdf += b"\nendobj\n"

    xref_start = len(pdf)
    pdf += f"xref\n0 {len(objects) + 1}\n".encode("ascii")
    pdf += b"0000000000 65535 f \n"

    for offset in offsets[1:]:
        pdf += f"{offset:010d} 00000 n \n".encode("ascii")

    pdf += b"trailer\n"
    pdf += f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n".encode("ascii")
    pdf += b"startxref\n"
    pdf += f"{xref_start}\n".encode("ascii")
    pdf += b"%%EOF"

    return pdf


def build_professional_pdf(report_data):
    reportlab_ready = importlib.util.find_spec("reportlab") is not None
    if not reportlab_ready:
        fallback_lines = []
        fallback_lines.append("SIMDF - Sistema Interno de Monitoreo y Deteccion de Fraude")
        fallback_lines.append(f"Reporte: {report_data['nombre_reporte']}")
        fallback_lines.append(f"Fecha y hora: {report_data['fecha_reporte']}")
        fallback_lines.append(f"Usuario: {report_data['usuario_solicitante']}")
        fallback_lines.append(f"Periodo: {report_data['periodo_reporte']}")
        fallback_lines.append("")
        fallback_lines.append("RESUMEN EJECUTIVO")
        fallback_lines.append(f"Transacciones analizadas: {report_data['total_consultas']}")
        fallback_lines.append(f"Fraudes detectados: {report_data['fraude_detectado']}")
        fallback_lines.append(f"Riesgo promedio: {report_data['riesgo_promedio']:.1f}%")
        fallback_lines.append(f"Alertas generadas: {report_data['alertas_generadas']}")
        fallback_lines.append(f"Modelo utilizado: {report_data['modelo_utilizado']}")
        fallback_lines.append(f"Riesgo alto: {report_data['riesgo_alto']}")
        fallback_lines.append(f"Riesgo medio: {report_data['riesgo_medio']}")
        fallback_lines.append(f"Riesgo bajo: {report_data['riesgo_bajo']}")
        fallback_lines.append("")
        fallback_lines.append("TABLA DE TRANSACCIONES")
        fallback_lines.append("ID | Fecha/Hora | Usuario | Monto | Riesgo | Prob. | Resultado | Accion")
        fallback_lines.append("-" * 78)
        for row in report_data["table_rows"][:16]:
            fallback_lines.append(
                (
                    f"{row['id']:<7} | {row['fecha_hora']:<16} | {row['usuario']:<10} | {row['monto']:<10} | "
                    f"{row['riesgo']:<5} | {row['prob']:<4} | {row['resultado']:<18} | {row['accion']:<18}"
                )
            )
        return build_simple_pdf("Reporte Ejecutivo SIMDF - Analitica de Fraude", fallback_lines)

    colors = importlib.import_module("reportlab.lib.colors")
    pagesizes = importlib.import_module("reportlab.lib.pagesizes")
    styles_mod = importlib.import_module("reportlab.lib.styles")
    units_mod = importlib.import_module("reportlab.lib.units")
    platypus_mod = importlib.import_module("reportlab.platypus")
    shapes_mod = importlib.import_module("reportlab.graphics.shapes")
    barcharts_mod = importlib.import_module("reportlab.graphics.charts.barcharts")
    piecharts_mod = importlib.import_module("reportlab.graphics.charts.piecharts")
    linecharts_mod = importlib.import_module("reportlab.graphics.charts.linecharts")

    A4 = pagesizes.A4
    getSampleStyleSheet = styles_mod.getSampleStyleSheet
    ParagraphStyle = styles_mod.ParagraphStyle
    mm = units_mod.mm
    SimpleDocTemplate = platypus_mod.SimpleDocTemplate
    Paragraph = platypus_mod.Paragraph
    Spacer = platypus_mod.Spacer
    Table = platypus_mod.Table
    TableStyle = platypus_mod.TableStyle
    PageBreak = platypus_mod.PageBreak
    Drawing = shapes_mod.Drawing
    String = shapes_mod.String
    Circle = shapes_mod.Circle
    VerticalBarChart = barcharts_mod.VerticalBarChart
    Pie = piecharts_mod.Pie
    HorizontalLineChart = linecharts_mod.HorizontalLineChart

    output = BytesIO()
    doc = SimpleDocTemplate(
        output,
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=14 * mm,
        title="Reporte Ejecutivo SIMDF",
        author="SIMDF",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "SimdfTitle",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=19,
        textColor=colors.HexColor("#0B2E4F"),
        leading=24,
        spaceAfter=10,
    )
    section_style = ParagraphStyle(
        "SimdfSection",
        parent=styles["Heading3"],
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=colors.HexColor("#0B2E4F"),
        spaceBefore=8,
        spaceAfter=6,
    )
    body_style = ParagraphStyle(
        "SimdfBody",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=12,
    )
    tiny_style = ParagraphStyle(
        "SimdfTiny",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=7.5,
        leading=10,
        textColor=colors.HexColor("#5A6C7E"),
    )

    story = []

    def draw_page_chrome(canvas, doc_obj):
        canvas.saveState()
        canvas.setFillColor(colors.HexColor("#0B2E4F"))
        canvas.rect(0, 821, 595, 21, fill=1, stroke=0)
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 8)
        canvas.drawString(22, 828, "SIMDF | Sistema Interno de Monitoreo y Deteccion de Fraude")
        canvas.setFillColor(colors.HexColor("#5A6C7E"))
        canvas.setFont("Helvetica", 7.5)
        canvas.drawString(22, 18, f"Reporte autogenerado: {report_data['fecha_reporte']}")
        canvas.drawRightString(575, 18, f"Pagina {doc_obj.page}")
        canvas.restoreState()

    logo_drawing = Drawing(32, 32)
    logo_circle = Circle(16, 16, 14)
    logo_circle.fillColor = colors.HexColor("#0B2E4F")
    logo_circle.strokeColor = colors.HexColor("#0B2E4F")
    logo_drawing.add(logo_circle)
    logo_drawing.add(String(8.4, 13.2, "S", fontName="Helvetica-Bold", fontSize=13, fillColor=colors.white))

    header_table = Table(
        [[logo_drawing, f"SIMDF\n{report_data['nombre_sistema']}"]],
        colWidths=[22 * mm, 148 * mm],
    )
    header_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#EAF1F8")),
                ("BOX", (0, 0), (-1, -1), 1, colors.HexColor("#BFD2E6")),
                ("FONTNAME", (0, 0), (0, 0), "Helvetica-Bold"),
                ("FONTNAME", (1, 0), (1, 0), "Helvetica"),
                ("FONTSIZE", (0, 0), (0, 0), 12),
                ("FONTSIZE", (1, 0), (1, 0), 9),
                ("TEXTCOLOR", (0, 0), (0, 0), colors.HexColor("#0B2E4F")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(header_table)
    story.append(Spacer(1, 8))
    story.append(Paragraph(report_data["nombre_reporte"], title_style))
    story.append(
        Paragraph(
            (
                f"<b>Fecha y hora de generacion:</b> {report_data['fecha_reporte']} &nbsp;&nbsp; "
                f"<b>Usuario que genero:</b> {report_data['usuario_solicitante']}"
            ),
            body_style,
        )
    )
    story.append(Paragraph(f"<b>Periodo analizado:</b> {report_data['periodo_reporte']}", body_style))
    story.append(Spacer(1, 8))

    hero_table = Table(
        [
            [
                Paragraph("<b>Estado General</b><br/>Monitoreo activo de fraude transaccional", body_style),
                Paragraph(
                    f"<b>Tasa de fraude</b><br/><font size='16'><b>{report_data['tasa_fraude']:.1f}%</b></font>",
                    body_style,
                ),
            ]
        ],
        colWidths=[118 * mm, 52 * mm],
    )
    hero_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F2F7FC")),
                ("BOX", (0, 0), (-1, -1), 1, colors.HexColor("#C1D3E6")),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(hero_table)
    story.append(Spacer(1, 8))

    story.append(Paragraph("Resumen Ejecutivo", section_style))
    story.append(
        Paragraph(
            (
                f"Se analizaron <b>{report_data['total_consultas']}</b> transacciones. "
                f"El sistema identifico <b>{report_data['fraude_detectado']}</b> eventos de alto riesgo "
                f"y una tasa estimada de fraude de <b>{report_data['tasa_fraude']:.1f}%</b>."
            ),
            body_style,
        )
    )
    story.append(
        Paragraph(
            (
                f"<b>Filtros aplicados:</b> {report_data['filtros_aplicados']} &nbsp;&nbsp; "
                f"<b>Periodo analizado:</b> {report_data['periodo_reporte']}"
            ),
            tiny_style,
        )
    )

    kpi_table = Table(
        [
            ["Metricas", "Valor"],
            ["Transacciones analizadas", str(report_data["total_consultas"])],
            ["Fraudes detectados", str(report_data["fraude_detectado"])],
            ["Riesgo promedio", f"{report_data['riesgo_promedio']:.1f}%"],
            ["Alertas generadas", str(report_data["alertas_generadas"])],
            ["Modelo utilizado", report_data["modelo_utilizado"]],
            ["Riesgo alto / medio / bajo", f"{report_data['riesgo_alto']} / {report_data['riesgo_medio']} / {report_data['riesgo_bajo']}"],
            ["Precision modelo LightGBM", f"{report_data['precision_modelo']}%"],
            ["Recall / F1 / AUC", f"{report_data['recall_modelo']}% / {report_data['f1_modelo']}% / {report_data['auc_modelo']}"],
        ],
        colWidths=[70 * mm, 100 * mm],
    )
    kpi_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0B2E4F")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.7),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#C5D2E0")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#F7FAFD"), colors.white]),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(Spacer(1, 6))
    story.append(kpi_table)

    story.append(Paragraph("Indicadores Visuales", section_style))

    risk_dist_text = Table(
        [
            ["Distribucion de riesgo"],
            [f"Riesgo alto: {report_data['riesgo_alto']}"],
            [f"Riesgo medio: {report_data['riesgo_medio']}"],
            [f"Riesgo bajo: {report_data['riesgo_bajo']}"],
        ],
        colWidths=[170 * mm],
    )
    risk_dist_text.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0E3A5F")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#C8D7E7")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#F7FBFF"), colors.white]),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    story.append(risk_dist_text)

    distribution_chart = Drawing(255, 140)
    bar = VerticalBarChart()
    bar.x = 22
    bar.y = 28
    bar.height = 88
    bar.width = 210
    bar.data = [[report_data["riesgo_bajo"], report_data["riesgo_medio"], report_data["riesgo_alto"]]]
    bar.categoryAxis.categoryNames = ["Bajo", "Medio", "Alto"]
    bar.bars[0].fillColor = colors.HexColor("#3A7CA5")
    bar.valueAxis.valueMin = 0
    bar.valueAxis.valueMax = max(1, report_data["riesgo_alto"], report_data["riesgo_medio"], report_data["riesgo_bajo"]) + 2
    bar.valueAxis.valueStep = max(1, int(bar.valueAxis.valueMax / 4))
    distribution_chart.add(bar)
    distribution_chart.add(String(24, 122, "Distribucion de riesgo", fontName="Helvetica-Bold", fontSize=9))

    pie_chart = Drawing(255, 140)
    pie = Pie()
    pie.x = 74
    pie.y = 24
    pie.width = 110
    pie.height = 92
    pie.data = [max(0, report_data["fraude_detectado"]), max(0, report_data["transacciones_normales"])]
    pie.labels = ["Fraude", "Normal"]
    pie.slices[0].fillColor = colors.HexColor("#D9534F")
    pie.slices[1].fillColor = colors.HexColor("#5CB85C")
    pie.sideLabels = True
    pie_chart.add(pie)
    pie_chart.add(String(24, 122, "Fraude vs normal", fontName="Helvetica-Bold", fontSize=9))

    charts_table = Table([[distribution_chart, pie_chart]], colWidths=[85 * mm, 85 * mm])
    charts_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
    story.append(charts_table)

    hourly_chart = Drawing(510, 165)
    line_chart = HorizontalLineChart()
    line_chart.x = 30
    line_chart.y = 28
    line_chart.height = 100
    line_chart.width = 455
    line_chart.data = [report_data["hourly_fraud"]]
    line_chart.categoryAxis.categoryNames = report_data["hourly_labels"]
    line_chart.categoryAxis.labels.angle = 45
    line_chart.categoryAxis.labels.dy = -10
    line_chart.valueAxis.valueMin = 0
    line_chart.valueAxis.valueMax = max(2, max(report_data["hourly_fraud"]) + 1)
    line_chart.valueAxis.valueStep = max(1, int(line_chart.valueAxis.valueMax / 4))
    line_chart.lines[0].strokeColor = colors.HexColor("#D9534F")
    line_chart.lines[0].strokeWidth = 1.8
    line_chart.lines[0].symbol = None
    hourly_chart.add(line_chart)
    hourly_chart.add(String(32, 135, "Tendencia de eventos de alto riesgo por hora", fontName="Helvetica-Bold", fontSize=9))
    story.append(hourly_chart)

    commerce_chart = Drawing(510, 145)
    commerce_bar = VerticalBarChart()
    commerce_bar.x = 32
    commerce_bar.y = 24
    commerce_bar.height = 85
    commerce_bar.width = 450
    commerce_bar.data = [report_data["commerce_values"]]
    commerce_bar.categoryAxis.categoryNames = report_data["commerce_labels"]
    commerce_bar.bars[0].fillColor = colors.HexColor("#2E73A8")
    commerce_bar.valueAxis.valueMin = 0
    commerce_bar.valueAxis.valueMax = max(2, max(report_data["commerce_values"]) + 1)
    commerce_bar.valueAxis.valueStep = max(1, int(commerce_bar.valueAxis.valueMax / 4))
    commerce_chart.add(commerce_bar)
    commerce_chart.add(String(34, 120, "Concentracion de riesgo por tipo de comercio", fontName="Helvetica-Bold", fontSize=9))
    story.append(commerce_chart)

    story.append(Paragraph("Top Factores de Riesgo", section_style))
    factors_rows = [["Factor", "Incidencia"]]
    for item in report_data["top_factors"]:
        factors_rows.append([item["name"], f"{item['percent']:.1f}%"])
    factors_table = Table(factors_rows, colWidths=[125 * mm, 45 * mm])
    factors_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#234A70")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8.5),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#D4DEE9")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#F8FBFF"), colors.white]),
            ]
        )
    )
    story.append(factors_table)

    story.append(Paragraph("Alertas Generadas", section_style))
    if report_data["alerts_rows"]:
        alerts_table_rows = [["Transaccion", "Detalle de alerta"]]
        for alert in report_data["alerts_rows"]:
            alerts_table_rows.append([alert["tx"], f"{alert['tx']} -> Riesgo {alert['riesgo']} ({alert['prob']}) | {alert['accion']}"])
        alerts_table = Table(alerts_table_rows, colWidths=[28 * mm, 142 * mm])
        alerts_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#6A1F1C")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#C7B6B5")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#FFF6F5"), colors.white]),
                ]
            )
        )
        story.append(alerts_table)
    else:
        story.append(Paragraph("No se registraron alertas de alto riesgo en el periodo filtrado.", body_style))

    story.append(PageBreak())

    story.append(Paragraph("Tabla de Transacciones Analizadas", section_style))
    tx_rows = [["ID", "Fecha / hora", "Usuario", "Monto", "Riesgo", "Probabilidad", "Resultado", "Accion recomendada"]]
    for row in report_data["table_rows"]:
        tx_rows.append(
            [
                row["id"],
                row["fecha_hora"],
                row["usuario"],
                row["monto"],
                row["riesgo"],
                row["prob"],
                row["resultado"],
                row["accion"],
            ]
        )

    tx_table = Table(
        tx_rows,
        colWidths=[16 * mm, 28 * mm, 22 * mm, 20 * mm, 15 * mm, 19 * mm, 26 * mm, 24 * mm],
        repeatRows=1,
    )
    tx_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0B2E4F")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#C9D8E8")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#F4F8FC"), colors.white]),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    story.append(tx_table)
    story.append(Spacer(1, 8))
    story.append(
        Paragraph(
            "SIMDF - Reporte tecnico autogenerado para seguimiento de riesgo transaccional. Universidad XXXXX - 2026.",
            tiny_style,
        )
    )

    doc.build(story, onFirstPage=draw_page_chrome, onLaterPages=draw_page_chrome)
    return output.getvalue()


def get_history_records_for_current_user(transaction_id=None, date_filter=None):
    conn = get_db_connection()

    date_sql = ""
    if date_filter == "today":
        date_sql = " AND date(c.created_at) = date('now', 'localtime') "
    elif date_filter == "7days":
        date_sql = " AND c.created_at >= datetime('now', '-7 day') "
    elif date_filter == "month":
        date_sql = " AND c.created_at >= datetime('now', '-1 month') "

    id_sql = ""
    extra_params = []
    if transaction_id:
        id_sql = " AND c.id = ? "
        extra_params.append(transaction_id)

    if session.get("role") == "administrador":
        consultas = conn.execute(
            f"""
            SELECT c.id, u.username, c.monto, c.tipo_comercio, c.metodo_pago, c.ubicacion,
                     c.frecuencia, c.hora, c.score, c.probabilidad_fraude, c.nivel_riesgo,
                     c.recomendacion, c.resultado, c.created_at, c.modelo
            FROM consultas c
            JOIN users u ON u.id = c.user_id
            WHERE 1=1
            {date_sql}
            {id_sql}
            ORDER BY c.created_at DESC
            """,
            tuple(extra_params),
        ).fetchall()
    else:
        # Include API-ingested records stored under the technical ingestion user.
        visible_user_ids = list(
            dict.fromkeys(
                [
                    int(session["user_id"]),
                    int(get_api_ingestion_user_id()),
                ]
            )
        )
        user_placeholders = ", ".join(["?"] * len(visible_user_ids))
        params = list(visible_user_ids)
        params.extend(extra_params)
        consultas = conn.execute(
            f"""
            SELECT c.id, u.username, c.monto, c.tipo_comercio, c.metodo_pago, c.ubicacion,
                     c.frecuencia, c.hora, c.score, c.probabilidad_fraude, c.nivel_riesgo,
                     c.recomendacion, c.resultado, c.created_at, c.modelo
            FROM consultas c
            JOIN users u ON u.id = c.user_id
            WHERE c.user_id IN ({user_placeholders})
            {date_sql}
            {id_sql}
            ORDER BY c.created_at DESC
            """,
            tuple(params),
        ).fetchall()

    consultas_formateadas = []
    for consulta in consultas:
        item = dict(consulta)
        item["tipo_comercio"] = TIPO_COMERCIO_LABELS.get(
            item.get("tipo_comercio"), item.get("tipo_comercio", "")
        )
        item["metodo_pago"] = METODO_PAGO_LABELS.get(
            item.get("metodo_pago"), item.get("metodo_pago", "")
        )
        item["ubicacion"] = UBICACION_LABELS.get(
            item.get("ubicacion"), item.get("ubicacion", "")
        )
        item["created_at"] = format_db_datetime(item.get("created_at"))
        item["modelo"] = MODEL_LABELS.get(item.get("modelo"), "Random Forest")

        badge_class, badge_text = get_risk_badge_data(item.get("nivel_riesgo"))
        item["riesgo_badge_class"] = badge_class
        item["riesgo_badge_texto"] = badge_text

        consultas_formateadas.append(item)

    conn.close()
    return consultas_formateadas


def build_dashboard_context():
    conn = get_db_connection()

    if session.get("role") == "administrador":
        where_clause = ""
        params = ()
    else:
        # Include API-ingested records stored under the technical ingestion user.
        visible_user_ids = list(
            dict.fromkeys(
                [
                    int(session["user_id"]),
                    int(get_api_ingestion_user_id()),
                ]
            )
        )
        user_placeholders = ", ".join(["?"] * len(visible_user_ids))
        where_clause = f"WHERE c.user_id IN ({user_placeholders})"
        params = tuple(visible_user_ids)

    consultas = conn.execute(
        f"""
        SELECT c.id, u.username, c.monto, c.tipo_comercio, c.metodo_pago, c.ubicacion,
             c.geo_latitude, c.geo_longitude,
               c.frecuencia, c.hora, c.score, c.probabilidad_fraude, c.nivel_riesgo,
               c.recomendacion, c.resultado, c.created_at, c.modelo
        FROM consultas c
        JOIN users u ON u.id = c.user_id
        {where_clause}
        ORDER BY c.created_at DESC
        """,
        params,
    ).fetchall()

    alertas_hoy = conn.execute(
        f"""
        SELECT COUNT(*) AS total
        FROM consultas c
        {where_clause} {"AND" if where_clause else "WHERE"} c.probabilidad_fraude >= 70
        AND c.created_at >= datetime('now', '-1 day')
        """,
        params,
    ).fetchone()["total"]

    conn.close()

    consultas_data = []
    high_risk_datetimes = []
    for consulta in consultas:
        item = dict(consulta)
        item["ubicacion_raw"] = item.get("ubicacion")

        created_at_raw = item.get("created_at")
        created_dt = None
        if created_at_raw:
            try:
                created_dt = datetime.strptime(created_at_raw, "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                created_dt = None

        if int(item.get("probabilidad_fraude", 0) or 0) >= 70 and created_dt is not None:
            high_risk_datetimes.append(created_dt)

        item["tipo_comercio"] = TIPO_COMERCIO_LABELS.get(
            item.get("tipo_comercio"), item.get("tipo_comercio", "")
        )
        item["metodo_pago"] = METODO_PAGO_LABELS.get(
            item.get("metodo_pago"), item.get("metodo_pago", "")
        )
        item["ubicacion"] = UBICACION_LABELS.get(
            item.get("ubicacion"), item.get("ubicacion", "")
        )
        item["created_at"] = format_db_datetime(item.get("created_at"))
        badge_class, badge_text = get_risk_badge_data(item.get("nivel_riesgo"))
        item["riesgo_badge_class"] = badge_class
        item["riesgo_badge_texto"] = badge_text
        consultas_data.append(item)

    total_analizadas = len(consultas_data)
    alto = len([c for c in consultas_data if "alto" in (c.get("nivel_riesgo", "").lower())])
    medio = len([c for c in consultas_data if "medio" in (c.get("nivel_riesgo", "").lower())])
    bajo = len([c for c in consultas_data if "bajo" in (c.get("nivel_riesgo", "").lower())])

    fraude_detectado = round((alto / total_analizadas) * 100, 1) if total_analizadas else 0
    prob_promedio = (
        round(sum(c.get("probabilidad_fraude", 0) for c in consultas_data) / total_analizadas, 1)
        if total_analizadas
        else 0
    )

    if prob_promedio >= 70:
        riesgo_promedio = "ALTO"
    elif prob_promedio >= 40:
        riesgo_promedio = "MEDIO"
    else:
        riesgo_promedio = "BAJO"

    riesgo_pct = {
        "alto": round((alto / total_analizadas) * 100, 1) if total_analizadas else 0,
        "medio": round((medio / total_analizadas) * 100, 1) if total_analizadas else 0,
        "bajo": round((bajo / total_analizadas) * 100, 1) if total_analizadas else 0,
    }

    day_counts = {}
    week_counts = {}
    month_counts = {}
    for dt in high_risk_datetimes:
        day_key = dt.date()
        day_counts[day_key] = day_counts.get(day_key, 0) + 1

        week_key = day_key - timedelta(days=day_key.weekday())
        week_counts[week_key] = week_counts.get(week_key, 0) + 1

        month_key = (day_key.year, day_key.month)
        month_counts[month_key] = month_counts.get(month_key, 0) + 1

    today = datetime.now().date()

    day_labels = []
    day_values = []
    for offset in range(6, -1, -1):
        current_day = today - timedelta(days=offset)
        day_labels.append(current_day.strftime("%d/%m"))
        day_values.append(day_counts.get(current_day, 0))

    current_week_start = today - timedelta(days=today.weekday())
    week_labels = []
    week_values = []
    for offset in range(7, -1, -1):
        week_start = current_week_start - timedelta(weeks=offset)
        week_labels.append(f"Sem {week_start.strftime('%d/%m')}")
        week_values.append(week_counts.get(week_start, 0))

    month_labels = []
    month_values = []
    for offset in range(5, -1, -1):
        month_num = today.month - offset
        year_num = today.year
        while month_num <= 0:
            month_num += 12
            year_num -= 1
        month_labels.append(f"{month_num:02d}/{year_num}")
        month_values.append(month_counts.get((year_num, month_num), 0))

    risk_distribution = {
        "alto": alto,
        "medio": medio,
        "bajo": bajo,
    }

    fraud_trend_series = {
        "day": {"labels": day_labels, "values": day_values},
        "week": {"labels": week_labels, "values": week_values},
        "month": {"labels": month_labels, "values": month_values},
    }

    geo_location_groups = {}

    for consulta in consultas_data:
        lat = consulta.get("geo_latitude")
        lng = consulta.get("geo_longitude")
        if lat is None or lng is None:
            continue

        try:
            lat = float(lat)
            lng = float(lng)
        except (TypeError, ValueError):
            continue

        raw_label = str(consulta.get("ubicacion_raw") or consulta.get("ubicacion") or "").strip()
        normalized_raw = normalize_csv_key(raw_label)

        # If location is a broad bucket (local/nacional/internacional), infer a concrete city from coordinates.
        if normalized_raw in UBICACION_LABELS:
            geo_label = resolve_geo_label_from_coordinates(lat, lng)
            city_label = geo_label.split(",")[0].strip() if geo_label else UBICACION_LABELS.get(normalized_raw, "Ubicacion detectada")
        else:
            city_label = raw_label.split(",")[0].strip() if "," in raw_label else raw_label

        if not city_label:
            city_label = "Ubicacion detectada"

        bucket = geo_location_groups.get(city_label)
        if not bucket:
            bucket = {"alerts": 0, "sum_lat": 0.0, "sum_lng": 0.0}

        bucket["alerts"] += 1
        bucket["sum_lat"] += lat
        bucket["sum_lng"] += lng
        geo_location_groups[city_label] = bucket

    sorted_locations = sorted(
        geo_location_groups.items(),
        key=lambda item: item[1]["alerts"],
        reverse=True,
    )[:10]

    max_location_alerts = max([item[1]["alerts"] for item in sorted_locations], default=0)
    fraud_by_location = []
    fraud_map_points = []
    for city_label, aggregate in sorted_locations:
        alerts = aggregate["alerts"]

        bar_pct = (round((alerts / max_location_alerts) * 100) if max_location_alerts else 0)
        if bar_pct >= 85:
            bar_class = "fraud-bar-100"
        elif bar_pct >= 65:
            bar_class = "fraud-bar-75"
        elif bar_pct >= 35:
            bar_class = "fraud-bar-50"
        elif bar_pct > 0:
            bar_class = "fraud-bar-25"
        else:
            bar_class = "fraud-bar-0"

        fraud_by_location.append(
            {
                "city": city_label,
                "alerts": alerts,
                "bar_pct": bar_pct,
                "bar_class": bar_class,
            }
        )

        fraud_map_points.append(
            {
                "city": city_label,
                "alerts": alerts,
                "lat": round(aggregate["sum_lat"] / alerts, 6),
                "lng": round(aggregate["sum_lng"] / alerts, 6),
            }
        )

    fraud_by_location = fraud_by_location[:3]

    ultima_consulta = consultas_data[0] if consultas_data else None
    ultimas_consultas = consultas_data[:6]

    ultimas_probabilidades = [c.get("probabilidad_fraude", 0) for c in consultas_data[:8]][::-1]

    if ultima_consulta:
        factores = {
            "Monto Alto": min(100, round((ultima_consulta.get("monto", 0) / 2000) * 100)),
            "Ubicación Inusual": 92 if ultima_consulta.get("ubicacion") == "Internacional" else 38,
            "Hora Nocturna": 88 if (ultima_consulta.get("hora", 12) < 6 or ultima_consulta.get("hora", 12) > 22) else 35,
            "Frecuencia Reciente": min(100, round((ultima_consulta.get("frecuencia", 0) / 10) * 100)),
            "Método de Pago": 70 if ultima_consulta.get("metodo_pago") in ["Transferencia", "Billetera digital"] else 42,
        }
    else:
        factores = {
            "Monto Alto": 40,
            "Ubicación Inusual": 30,
            "Hora Nocturna": 25,
            "Frecuencia Reciente": 35,
            "Método de Pago": 28,
        }

    explainability_factors = [
        {"label": label, "impact": impact}
        for label, impact in sorted(factores.items(), key=lambda item: item[1], reverse=True)[:4]
    ]

    riesgo_destacado = {
        "texto": (ultima_consulta.get("nivel_riesgo") if ultima_consulta else "Sin riesgo calculado"),
        "probabilidad": (ultima_consulta.get("probabilidad_fraude") if ultima_consulta else 0),
        "recomendacion": (ultima_consulta.get("recomendacion") if ultima_consulta else "Realiza una nueva simulación"),
    }

    riesgo_lower = riesgo_destacado["texto"].lower()
    if "alto" in riesgo_lower:
        riesgo_destacado["class"] = "risk-hero-alto"
    elif "medio" in riesgo_lower:
        riesgo_destacado["class"] = "risk-hero-medio"
    else:
        riesgo_destacado["class"] = "risk-hero-bajo"

    settings = get_app_settings()
    active_model = FIXED_MODEL_KEY

    model_catalog = get_model_catalog()
    metrics_scope_user = None if session.get("role") == "administrador" else session.get("user_id")
    dynamic_metrics = get_dynamic_model_metrics(active_model, user_id=metrics_scope_user)

    if dynamic_metrics:
        active_metrics = dynamic_metrics["metrics"]
        confusion_matrix = dynamic_metrics["confusion"]
    else:
        active_metrics = model_catalog.get(active_model, model_catalog[FIXED_MODEL_KEY])
        confusion_matrix = {
            "tn": 412,
            "fp": 48,
            "fn": 39,
            "tp": 356,
        }

    return {
        "total_analizadas": total_analizadas,
        "fraude_detectado": fraude_detectado,
        "risk_distribution": risk_distribution,
        "fraud_trend_series": fraud_trend_series,
        "riesgo_promedio": riesgo_promedio,
        "prob_promedio": prob_promedio,
        "alertas_hoy": alertas_hoy,
        "fraud_by_location": fraud_by_location,
        "fraud_map_points": fraud_map_points,
        "riesgo_pct": riesgo_pct,
        "ultima_consulta": ultima_consulta,
        "ultimas_consultas": ultimas_consultas,
        "ultimas_probabilidades": ultimas_probabilidades,
        "model_metrics": active_metrics,
        "model_catalog": model_catalog,
        "active_model": active_model,
        "active_model_label": MODEL_LABELS.get(active_model, "Random Forest"),
        "active_model_metrics": active_metrics,
        "confusion_matrix": confusion_matrix,
        "explainability_factors": explainability_factors,
        "riesgo_destacado": riesgo_destacado,
    }


def init_db():
    os.makedirs(PROFILE_UPLOAD_DIR, exist_ok=True)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('analista', 'administrador')),
            blocked_until TEXT,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            department TEXT,
            bio TEXT,
            employee_code TEXT,
            office_location TEXT,
            alert_channel TEXT,
            profile_photo_url TEXT,
            notif_high_risk INTEGER NOT NULL DEFAULT 1,
            notif_critical_alerts INTEGER NOT NULL DEFAULT 1,
            notif_analysis_complete INTEGER NOT NULL DEFAULT 1,
            notif_new_transactions INTEGER NOT NULL DEFAULT 1,
            notif_user_activity INTEGER NOT NULL DEFAULT 0,
            notif_in_app INTEGER NOT NULL DEFAULT 1,
            notif_frequency TEXT NOT NULL DEFAULT 'immediate',
            security_two_factor INTEGER NOT NULL DEFAULT 0,
            security_notify_login INTEGER NOT NULL DEFAULT 1,
            security_notify_failed INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            session_version INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS consultas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            monto REAL NOT NULL,
            tipo_comercio TEXT NOT NULL DEFAULT '',
            metodo_pago TEXT NOT NULL DEFAULT '',
            ubicacion TEXT NOT NULL DEFAULT '',
            geo_latitude REAL,
            geo_longitude REAL,
            frecuencia REAL NOT NULL,
            hora REAL NOT NULL,
            score REAL NOT NULL,
            probabilidad_fraude INTEGER NOT NULL DEFAULT 0,
            true_fraud_label INTEGER,
            nivel_riesgo TEXT NOT NULL DEFAULT '',
            recomendacion TEXT NOT NULL DEFAULT '',
            modelo TEXT NOT NULL DEFAULT 'lightgbm',
            resultado TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            severity TEXT NOT NULL DEFAULT 'INFO',
            message TEXT NOT NULL,
            actor_username TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS login_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            ip_address TEXT NOT NULL,
            success INTEGER NOT NULL DEFAULT 0,
            failure_reason TEXT,
            user_agent TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blocked_ips (
            ip_address TEXT PRIMARY KEY,
            reason TEXT NOT NULL,
            blocked_until TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            target_table TEXT NOT NULL DEFAULT 'consultas',
            records_imported INTEGER NOT NULL DEFAULT 0,
            records_skipped INTEGER NOT NULL DEFAULT 0,
            uploaded_by TEXT NOT NULL,
            uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS api_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            genero TEXT NOT NULL,
            edad INTEGER NOT NULL,
            ciudad TEXT NOT NULL,
            tipo_cuenta TEXT NOT NULL,
            monto_usd REAL NOT NULL,
            tipo_transaccion TEXT NOT NULL,
            categoria_comercio TEXT NOT NULL,
            balance_cuenta_usd REAL NOT NULL,
            dispositivo_transaccion TEXT NOT NULL,
            tipo_dispositivo TEXT NOT NULL,
            porcentaje_gasto REAL NOT NULL,
            transaccion_grande INTEGER NOT NULL,
            saldo_restante REAL NOT NULL,
            compra_riesgosa INTEGER NOT NULL,
            riesgo_edad_monto REAL NOT NULL,
            dia_semana INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            hora INTEGER NOT NULL,
            transaccion_nocturna INTEGER NOT NULL,
            model_prediction INTEGER,
            risk_score INTEGER NOT NULL,
            clasificacion TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    user_columns = {row["name"] for row in cursor.execute("PRAGMA table_info(users)").fetchall()}
    if "blocked_until" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN blocked_until TEXT")
    if "full_name" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN full_name TEXT")
    if "email" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN email TEXT")
    if "phone" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN phone TEXT")
    if "department" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN department TEXT")
    if "bio" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN bio TEXT")
    if "employee_code" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN employee_code TEXT")
    if "office_location" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN office_location TEXT")
    if "alert_channel" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN alert_channel TEXT")
    if "profile_photo_url" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN profile_photo_url TEXT")
    if "notif_high_risk" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN notif_high_risk INTEGER NOT NULL DEFAULT 1")
    if "notif_critical_alerts" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN notif_critical_alerts INTEGER NOT NULL DEFAULT 1")
    if "notif_analysis_complete" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN notif_analysis_complete INTEGER NOT NULL DEFAULT 1")
    if "notif_new_transactions" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN notif_new_transactions INTEGER NOT NULL DEFAULT 1")
    if "notif_user_activity" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN notif_user_activity INTEGER NOT NULL DEFAULT 0")
    if "notif_in_app" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN notif_in_app INTEGER NOT NULL DEFAULT 1")
    if "notif_frequency" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN notif_frequency TEXT NOT NULL DEFAULT 'immediate'")
    if "security_two_factor" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN security_two_factor INTEGER NOT NULL DEFAULT 0")
    if "security_notify_login" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN security_notify_login INTEGER NOT NULL DEFAULT 1")
    if "security_notify_failed" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN security_notify_failed INTEGER NOT NULL DEFAULT 1")
    if "theme_preference" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN theme_preference TEXT")
    if "interface_size" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN interface_size TEXT")
    if "created_at" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
        cursor.execute(
            "UPDATE users SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"
        )
    if "session_version" not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN session_version INTEGER NOT NULL DEFAULT 0")

    consultas_columns = {
        row["name"] for row in cursor.execute("PRAGMA table_info(consultas)").fetchall()
    }
    login_attempts_columns = {
        row["name"] for row in cursor.execute("PRAGMA table_info(login_attempts)").fetchall()
    }
    if "user_agent" not in login_attempts_columns:
        cursor.execute("ALTER TABLE login_attempts ADD COLUMN user_agent TEXT")

    if "tipo_comercio" not in consultas_columns:
        cursor.execute(
            "ALTER TABLE consultas ADD COLUMN tipo_comercio TEXT NOT NULL DEFAULT ''"
        )
    if "metodo_pago" not in consultas_columns:
        cursor.execute(
            "ALTER TABLE consultas ADD COLUMN metodo_pago TEXT NOT NULL DEFAULT ''"
        )
    if "ubicacion" not in consultas_columns:
        cursor.execute("ALTER TABLE consultas ADD COLUMN ubicacion TEXT NOT NULL DEFAULT ''")
    if "geo_latitude" not in consultas_columns:
        cursor.execute("ALTER TABLE consultas ADD COLUMN geo_latitude REAL")
    if "geo_longitude" not in consultas_columns:
        cursor.execute("ALTER TABLE consultas ADD COLUMN geo_longitude REAL")
    if "probabilidad_fraude" not in consultas_columns:
        cursor.execute(
            "ALTER TABLE consultas ADD COLUMN probabilidad_fraude INTEGER NOT NULL DEFAULT 0"
        )
    if "true_fraud_label" not in consultas_columns:
        cursor.execute("ALTER TABLE consultas ADD COLUMN true_fraud_label INTEGER")
    if "nivel_riesgo" not in consultas_columns:
        cursor.execute(
            "ALTER TABLE consultas ADD COLUMN nivel_riesgo TEXT NOT NULL DEFAULT ''"
        )
    if "recomendacion" not in consultas_columns:
        cursor.execute(
            "ALTER TABLE consultas ADD COLUMN recomendacion TEXT NOT NULL DEFAULT ''"
        )
    if "modelo" not in consultas_columns:
        cursor.execute(
            "ALTER TABLE consultas ADD COLUMN modelo TEXT NOT NULL DEFAULT 'lightgbm'"
        )

    dataset_uploads_columns = {
        row["name"] for row in cursor.execute("PRAGMA table_info(dataset_uploads)").fetchall()
    }
    if "target_table" not in dataset_uploads_columns:
        cursor.execute(
            "ALTER TABLE dataset_uploads ADD COLUMN target_table TEXT NOT NULL DEFAULT 'consultas'"
        )

    save_defaults = {
        "active_model": FIXED_MODEL_KEY,
        "fraud_block_threshold": "4",
        "block_minutes": "30",
        "alert_email_enabled": "1",
        "alert_recipient": "fraude@simdf.local",
        "language": "es",
        "theme": "light",
        "interface_size": "compact",
    }
    for key, value in save_defaults.items():
        cursor.execute(
            """
            INSERT OR IGNORE INTO app_settings (setting_key, setting_value)
            VALUES (?, ?)
            """,
            (key, value),
        )

    cursor.execute(
        "UPDATE app_settings SET setting_value = ? WHERE setting_key = 'active_model'",
        (FIXED_MODEL_KEY,),
    )

    # Always keep demo credentials available for analyst access.
    analista_hash = generate_password_hash("analista123")
    cursor.execute(
        """
        UPDATE users
        SET password_hash = ?, role = 'analista'
        WHERE LOWER(username) = 'analista'
        """,
        (analista_hash,),
    )
    if cursor.rowcount == 0:
        cursor.execute(
            """
            INSERT INTO users (username, password_hash, role)
            VALUES ('analista', ?, 'analista')
            """,
            (analista_hash,),
        )

    cursor.execute(
        """
        INSERT OR IGNORE INTO users (username, password_hash, role)
        VALUES (?, ?, ?)
        """,
        ("admin", generate_password_hash("admin123"), "administrador"),
    )

    conn.commit()
    conn.close()


def login_required(view_func):
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))

        conn = get_db_connection()
        row = conn.execute(
            "SELECT session_version FROM users WHERE id = ?",
            (session["user_id"],),
        ).fetchone()
        conn.close()

        if not row:
            session.clear()
            return redirect(url_for("login"))

        current_version = row["session_version"] or 0
        if session.get("session_version") is None:
            session["session_version"] = current_version
        elif session.get("session_version") != current_version:
            session.clear()
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapped_view


def roles_required(*allowed_roles):
    def decorator(view_func):
        @wraps(view_func)
        def wrapped_view(*args, **kwargs):
            user_role = session.get("role")
            if user_role not in allowed_roles:
                return redirect(url_for("home"))
            return view_func(*args, **kwargs)

        return wrapped_view

    return decorator


@app.context_processor
def inject_current_user():
    settings = get_app_settings()
    if "user_id" in session:
        user_ui = get_user_ui_preferences(session.get("user_id"))
        effective_settings = settings.copy()
        if user_ui.get("theme"):
            effective_settings["theme"] = user_ui["theme"]
        if user_ui.get("interface_size"):
            effective_settings["interface_size"] = user_ui["interface_size"]

        return {
            "current_user": {
                "id": session.get("user_id"),
                "username": session.get("username"),
                "role": session.get("role"),
            },
            "ui_settings": effective_settings,
            "format_usd": format_currency_usd,
        }
    return {"current_user": None, "ui_settings": settings, "format_usd": format_currency_usd}


def render_page(page, **context):
    meta = PAGE_META.get(page, PAGE_META["dashboard"])
    payload = {"page": page, **meta}
    payload.update(context)
    return render_template("index.html", **payload)


@app.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("home"))

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        ip_address = get_client_ip()
        user_agent = request.headers.get("User-Agent", "")

        active_block = get_active_ip_block(ip_address)
        if active_block:
            blocked_until_fmt = format_db_datetime(active_block["blocked_until"])
            error = f"IP bloqueada temporalmente hasta {blocked_until_fmt}"
            register_login_attempt(username, ip_address, False, "ip_blocked", user_agent)
            log_event(
                "SECURITY",
                f"Intento de login bloqueado desde IP {ip_address} para usuario {username or 'desconocido'}.",
                actor_username=username or "anonimo",
                severity="WARN",
            )
            return render_page("login", error=error)

        conn = get_db_connection()
        user = conn.execute(
            "SELECT id, username, password_hash, role, blocked_until, session_version FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        conn.close()

        if user and user["blocked_until"]:
            try:
                blocked_until = datetime.strptime(user["blocked_until"], "%Y-%m-%d %H:%M:%S")
                if blocked_until > datetime.now():
                    error = f"Usuario bloqueado temporalmente hasta {blocked_until.strftime('%d/%m/%Y %H:%M')}"
                    return render_page("login", error=error)
            except ValueError:
                pass

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            session["session_version"] = user["session_version"] or 0
            log_event(
                "AUTH",
                f"Inicio de sesión exitoso para {username} desde IP {ip_address}",
                actor_username=username,
                severity="INFO",
            )
            register_login_attempt(username, ip_address, True, "ok", user_agent)
            return redirect(url_for("home"))

        register_login_attempt(username, ip_address, False, "invalid_credentials", user_agent)
        failed_total = count_recent_failed_attempts(ip_address)
        if failed_total >= FAILED_LOGIN_MAX_ATTEMPTS:
            blocked_until = block_ip_address(
                ip_address,
                "Bloqueo automático por intentos fallidos repetidos",
                AUTO_BLOCK_MINUTES,
                "sistema",
            )
            error = f"Demasiados intentos fallidos. IP bloqueada hasta {format_db_datetime(blocked_until)}"
            log_event(
                "SECURITY",
                f"IP {ip_address} bloqueada automáticamente por {failed_total} intentos fallidos en {FAILED_LOGIN_WINDOW_MINUTES} minutos.",
                actor_username=username or "anonimo",
                severity="WARN",
            )
            return render_page("login", error=error)

        log_event(
            "AUTH",
            f"Intento de login fallido para {username or 'desconocido'} desde IP {ip_address}",
            actor_username=username or "anonimo",
            severity="WARN",
        )

        error = "Credenciales inválidas"

    return render_page("login", error=error)


@app.route("/registro", methods=["GET", "POST"])
def register():
    if "user_id" in session:
        return redirect(url_for("home"))

    error = None
    notice = None

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if len(username) < 3:
            error = "El usuario debe tener al menos 3 caracteres."
        elif not re.match(r"^[A-Za-z0-9_.-]{3,32}$", username):
            error = "El usuario solo puede contener letras, números, punto, guion y guion bajo."
        elif not is_valid_email(email):
            error = "Debes registrar un correo válido."
        elif password != confirm_password:
            error = "La confirmación de contraseña no coincide."
        else:
            strong, strong_error = validate_password_strength(password)
            if not strong:
                error = strong_error

        if not error:
            conn = get_db_connection()
            existing_username = conn.execute(
                "SELECT id FROM users WHERE LOWER(username) = LOWER(?)",
                (username,),
            ).fetchone()
            existing_email = conn.execute(
                "SELECT id FROM users WHERE LOWER(email) = LOWER(?)",
                (email,),
            ).fetchone()

            if existing_username:
                error = "El nombre de usuario ya existe."
                conn.close()
            elif existing_email:
                error = "El correo ya está asociado a otra cuenta."
                conn.close()
            else:
                conn.execute(
                    """
                    INSERT INTO users (username, password_hash, role, full_name, email)
                    VALUES (?, ?, 'analista', ?, ?)
                    """,
                    (username, generate_password_hash(password), full_name, email),
                )
                conn.commit()
                conn.close()
                notice = "Cuenta creada correctamente. Ahora puedes iniciar sesión."
                log_event("AUTH", f"Nuevo registro de usuario: {username}", actor_username=username, severity="INFO")

    return render_page("register", error=error, notice=notice)


@app.route("/recuperar-contrasena", methods=["GET", "POST"])
def forgot_password():
    if "user_id" in session:
        return redirect(url_for("home"))

    error = None
    notice = None

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip().lower()
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not is_valid_email(email):
            error = "Debes registrar un correo válido."
        elif new_password != confirm_password:
            error = "La confirmación de contraseña no coincide."
        else:
            strong, strong_error = validate_password_strength(new_password)
            if not strong:
                error = strong_error

        if not error:
            conn = get_db_connection()
            user = conn.execute(
                "SELECT id, username, email FROM users WHERE username = ?",
                (username,),
            ).fetchone()

            if not user:
                error = "No existe una cuenta con ese usuario."
                conn.close()
            elif (user["email"] or "").strip().lower() != email:
                error = "El correo no coincide con el usuario indicado."
                conn.close()
            else:
                conn.execute(
                    "UPDATE users SET password_hash = ? WHERE id = ?",
                    (generate_password_hash(new_password), user["id"]),
                )
                conn.commit()
                conn.close()
                notice = "Contraseña restablecida correctamente. Ya puedes iniciar sesión."
                log_event("SECURITY", f"Restablecimiento de contraseña para {username}", actor_username=username, severity="WARN")

    return render_page("forgot_password", error=error, notice=notice)


@app.route("/logout")
def logout():
    if session.get("username"):
        log_event(
            "AUTH",
            f"Cierre de sesión de {session.get('username')}",
            actor_username=session.get("username"),
            severity="INFO",
        )
    session.clear()
    return redirect(url_for("login"))


@app.route("/mi-perfil", methods=["GET", "POST"])
@login_required
def my_profile():
    status_message = None
    error = None
    notice = None

    if request.method == "POST":
        action = request.form.get("action", "update_profile")

        if action == "update_password":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")

            if len(new_password) < 8:
                error = "La nueva contraseña debe tener al menos 8 caracteres."
            elif new_password != confirm_password:
                error = "La confirmación de contraseña no coincide."
            else:
                conn = get_db_connection()
                user_row = conn.execute(
                    "SELECT password_hash FROM users WHERE id = ?",
                    (session["user_id"],),
                ).fetchone()

                if not user_row or not check_password_hash(user_row["password_hash"], current_password):
                    error = "La contraseña actual es incorrecta."
                    conn.close()
                else:
                    conn.execute(
                        "UPDATE users SET password_hash = ? WHERE id = ?",
                        (generate_password_hash(new_password), session["user_id"]),
                    )
                    conn.commit()
                    conn.close()
                    log_event("SECURITY", f"Contraseña actualizada por {session.get('username')}")
                    status_message = "Contraseña actualizada correctamente."

        elif action == "close_sessions":
            conn = get_db_connection()
            conn.execute(
                "UPDATE users SET session_version = COALESCE(session_version, 0) + 1 WHERE id = ?",
                (session["user_id"],),
            )
            row = conn.execute(
                "SELECT session_version FROM users WHERE id = ?",
                (session["user_id"],),
            ).fetchone()
            conn.commit()
            conn.close()

            session["session_version"] = (row["session_version"] if row else 0)
            log_event("SECURITY", f"Sesiones activas cerradas por {session.get('username')}", severity="WARN")
            notice = "Se cerraron las demás sesiones activas de esta cuenta."

        else:
            full_name = request.form.get("full_name", "").strip()
            email = request.form.get("email", "").strip()
            phone = request.form.get("phone", "").strip()
            department = request.form.get("department", "").strip()
            bio = request.form.get("bio", "").strip()
            employee_code = request.form.get("employee_code", "").strip()
            office_location = request.form.get("office_location", "").strip()
            alert_channel = request.form.get("alert_channel", "email").strip()
            current_profile_photo_url = request.form.get("current_profile_photo_url", "").strip()
            profile_photo_url = current_profile_photo_url
            remove_profile_photo = request.form.get("remove_profile_photo") == "1"
            profile_photo_file = request.files.get("profile_photo_file")

            if remove_profile_photo:
                profile_photo_url = ""

            if profile_photo_file and profile_photo_file.filename:
                if not is_allowed_profile_image(profile_photo_file.filename):
                    error = "Formato de imagen no permitido. Usa PNG, JPG, JPEG, WEBP o GIF."
                else:
                    os.makedirs(PROFILE_UPLOAD_DIR, exist_ok=True)
                    saved_filename = build_profile_image_filename(session["user_id"], profile_photo_file.filename)
                    save_path = os.path.join(PROFILE_UPLOAD_DIR, saved_filename)
                    profile_photo_file.save(save_path)
                    profile_photo_url = url_for("static", filename=f"uploads/profiles/{saved_filename}")

            if alert_channel not in ("email", "sms", "both"):
                alert_channel = "email"

            if not error:
                conn = get_db_connection()
                conn.execute(
                    """
                    UPDATE users
                    SET full_name = ?, email = ?, phone = ?, department = ?, bio = ?,
                        employee_code = ?, office_location = ?, alert_channel = ?, profile_photo_url = ?
                    WHERE id = ?
                    """,
                    (
                        full_name,
                        email,
                        phone,
                        department,
                        bio,
                        employee_code,
                        office_location,
                        alert_channel,
                        profile_photo_url,
                        session["user_id"],
                    ),
                )
                conn.commit()
                conn.close()

                log_event("PROFILE", f"Perfil actualizado por {session.get('username')}")
                status_message = "Perfil actualizado correctamente."

    conn = get_db_connection()
    profile = conn.execute(
        """
        SELECT id, username, role, full_name, email, phone, department, bio,
             blocked_until, employee_code, office_location, alert_channel,
             profile_photo_url, created_at
        FROM users
        WHERE id = ?
        """,
        (session["user_id"],),
    ).fetchone()

    profile_total = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM consultas
        WHERE user_id = ?
        """,
        (session["user_id"],),
    ).fetchone()["total"]

    profile_high_24h = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM consultas
        WHERE user_id = ?
          AND probabilidad_fraude >= 70
          AND created_at >= datetime('now', '-1 day')
        """,
        (session["user_id"],),
    ).fetchone()["total"]

    profile_week = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM consultas
        WHERE user_id = ?
          AND created_at >= datetime('now', '-7 day')
        """,
        (session["user_id"],),
    ).fetchone()["total"]

    access_rows = conn.execute(
        """
        SELECT severity, message, created_at
        FROM system_logs
        WHERE event_type = 'AUTH'
          AND (
              actor_username = ?
              OR message LIKE ?
          )
        ORDER BY created_at DESC
        LIMIT 8
        """,
        (session.get("username", ""), f"%{session.get('username', '')}%"),
    ).fetchall()
    conn.close()

    profile_data = dict(profile) if profile else {}
    if profile_data.get("blocked_until"):
        profile_data["blocked_until_fmt"] = format_db_datetime(profile_data.get("blocked_until"))
    else:
        profile_data["blocked_until_fmt"] = ""
    if profile_data.get("created_at"):
        profile_data["created_at_fmt"] = format_db_datetime(profile_data.get("created_at"))
    else:
        profile_data["created_at_fmt"] = ""
    if not profile_data.get("alert_channel"):
        profile_data["alert_channel"] = "email"

    profile_stats = {
        "total_consultas": profile_total or 0,
        "consultas_7d": profile_week or 0,
        "fraudes_altos_24h": profile_high_24h or 0,
    }

    access_logs = []
    for row in access_rows:
        access_logs.append(
            {
                "created_at": format_db_datetime(row["created_at"]),
                "severity": row["severity"],
                "message": row["message"],
            }
        )

    return render_page(
        "profile",
        profile=profile_data,
        profile_stats=profile_stats,
        access_logs=access_logs,
        status_message=status_message,
        error=error,
        notice=notice,
    )


@app.route("/")
@login_required
def home():
    return render_page("dashboard", **build_dashboard_context())


@app.route("/nueva-transaccion")
@login_required
def new_transaction():
    return redirect(url_for("home"))


@app.route("/metricas-modelo")
@login_required
def model_metrics_page():
    context = build_dashboard_context()
    selected_model = FIXED_MODEL_KEY
    recalc_requested = request.args.get("recalcular", "0") == "1"
    metrics_scope_user = None if session.get("role") == "administrador" else session.get("user_id")
    dynamic_metrics = get_dynamic_model_metrics(selected_model, user_id=metrics_scope_user)

    using_global_fallback = False
    if not dynamic_metrics and metrics_scope_user is not None:
        dynamic_metrics = get_dynamic_model_metrics(selected_model, user_id=None)
        using_global_fallback = dynamic_metrics is not None

    if dynamic_metrics:
        selected_metrics = dynamic_metrics["metrics"]
        confusion_matrix = dynamic_metrics["confusion"]
        if recalc_requested:
            metrics_notice = "Métricas recalculadas correctamente"
        else:
            metrics_notice = "Métricas calculadas correctamente con el dataset actual."
            if using_global_fallback:
                metrics_notice += " Se muestra el consolidado global del dataset montado."
    else:
        selected_metrics = context["model_catalog"].get(selected_model, context["active_model_metrics"])
        confusion_matrix = context["confusion_matrix"]
        if has_mounted_dataset_uploads():
            metrics_notice = (
                "Dataset montado detectado, pero sin etiquetas de fraude compatibles para calcular métricas reales. "
                "Mostrando métricas base del modelo."
            )
        else:
            metrics_notice = "Aún no hay dataset montado con etiquetas reales. Mostrando métricas base del modelo."
        if recalc_requested:
            metrics_notice = "Recalculo ejecutado, pero no hay suficientes etiquetas válidas para métricas dinámicas. " + metrics_notice

    return render_page(
        "model_metrics",
        model_metrics=selected_metrics,
        model_choices=MODEL_LABELS,
        selected_model=selected_model,
        model_catalog=context["model_catalog"],
        roc_curve=selected_metrics["roc_curve"],
        confusion_matrix=confusion_matrix,
        metrics_notice=metrics_notice,
        model_selection_locked=True,
        fixed_model_label=MODEL_LABELS.get(selected_model, "LightGBM"),
    )


@app.route("/explicabilidad")
@login_required
def explainability_page():
    context = build_dashboard_context()
    return render_page(
        "explainability",
        explainability_factors=context["explainability_factors"],
        riesgo_destacado=context["riesgo_destacado"],
        ultima_consulta=context["ultima_consulta"],
        shap_summary="Las variables que más influyeron fueron: monto, ubicación, frecuencia y horario de operación.",
    )


@app.route("/acerca")
@login_required
def about_page():
    return render_page("about")


@app.route("/predict", methods=["POST"])
@login_required
def predict():
    geo_location_label = request.form.get("geo_location_label", "").strip()
    geo_latitude_raw = request.form.get("geo_latitude", "").strip()
    geo_longitude_raw = request.form.get("geo_longitude", "").strip()
    hora = parse_hour_for_prediction(request.form.get("hora"))
    if hora is None:
        hora = parse_hour_for_prediction(request.form.get("real_transaction_hour"))
    if hora is None:
        hora = get_current_decimal_hour()

    monto = float(request.form["model_monto_usd"])

    artifact_features = {
        "Genero": request.form["model_genero"],
        "Edad": float(request.form["model_edad"]),
        "Ciudad": request.form["model_ciudad"],
        "Tipo_Cuenta": request.form["model_tipo_cuenta"],
        "Monto_USD": monto,
        "Tipo_Transaccion": request.form["model_tipo_transaccion"],
        "Categoria_Comercio": request.form["model_categoria_comercio"],
        "Balance_Cuenta_USD": float(request.form["model_balance_cuenta_usd"]),
        "Dispositivo_Transaccion": request.form["model_dispositivo_transaccion"],
        "Tipo_Dispositivo": request.form["model_tipo_dispositivo"],
        "Porcentaje_Gasto": float(request.form["model_porcentaje_gasto"]),
        "Transaccion_Grande": int(request.form["model_transaccion_grande"]),
        "Saldo_Restante": float(request.form["model_saldo_restante"]),
        "Compra_Riesgosa": int(request.form["model_compra_riesgosa"]),
        "Riesgo_Edad_Monto": float(request.form["model_riesgo_edad_monto"]),
        "Dia_Semana": int(request.form["model_dia_semana"]),
        "Mes": int(request.form["model_mes"]),
        "Hora": float(request.form["model_hora"]),
        "Transaccion_Nocturna": int(request.form["model_transaccion_nocturna"]),
    }

    tipo_comercio = map_model_categoria_to_legacy_comercio(artifact_features["Categoria_Comercio"])
    metodo_pago = map_model_tipo_tx_to_legacy_metodo(artifact_features["Tipo_Transaccion"])
    ubicacion = map_geo_label_to_legacy_ubicacion(geo_location_label)
    frecuencia = max(0.0, float(artifact_features["Porcentaje_Gasto"]) / 10.0)

    try:
        geo_latitude = float(geo_latitude_raw) if geo_latitude_raw else None
    except ValueError:
        geo_latitude = None

    try:
        geo_longitude = float(geo_longitude_raw) if geo_longitude_raw else None
    except ValueError:
        geo_longitude = None

    stored_location = geo_location_label or UBICACION_LABELS.get(ubicacion, ubicacion)

    if geo_latitude is None or geo_longitude is None or not geo_location_label:
        return render_page(
            "new_transaction",
            fixed_model_label=MODEL_LABELS.get(FIXED_MODEL_KEY, "LightGBM"),
            geo_error="Debes permitir y detectar tu ubicación real antes de enviar la transacción.",
        )

    selected_model = FIXED_MODEL_KEY

    selected_result = evaluate_risk_by_model(
        monto,
        tipo_comercio,
        metodo_pago,
        ubicacion,
        frecuencia,
        hora,
        selected_model,
        artifact_features=artifact_features,
    )

    model_comparison = [
        evaluate_risk_by_model(
            monto,
            tipo_comercio,
            metodo_pago,
            ubicacion,
            frecuencia,
            hora,
            FIXED_MODEL_KEY,
            artifact_features=artifact_features,
        )
    ]

    score = selected_result["score"]
    probabilidad_fraude = selected_result["probabilidad_fraude"]
    nivel_riesgo = selected_result["nivel_riesgo"]
    recomendacion = selected_result["recomendacion"]
    clase = selected_result["clase"]

    conn = get_db_connection()
    resultado = store_consulta_record(
        conn=conn,
        user_id=session["user_id"],
        monto=monto,
        tipo_comercio=tipo_comercio,
        metodo_pago=metodo_pago,
        ubicacion=stored_location,
        frecuencia=frecuencia,
        hora=hora,
        score=score,
        probabilidad_fraude=probabilidad_fraude,
        nivel_riesgo=nivel_riesgo,
        recomendacion=recomendacion,
        selected_model=selected_model,
        geo_latitude=geo_latitude,
        geo_longitude=geo_longitude,
    )
    conn.commit()
    conn.close()

    if clase == "alto":
        log_event(
            "FRAUD_ALERT",
            f"Alerta de fraude alto en transacción de {session.get('username')} con {MODEL_LABELS.get(selected_model)}.",
            severity="WARN",
        )

        if get_app_settings().get("alert_email_enabled", "1") == "1":
            alert_recipient = get_app_settings().get("alert_recipient", "fraude@simdf.local")
            log_event(
                "MAIL",
                f"Simulación de correo enviada a {alert_recipient}: transacción sospechosa detectada.",
                severity="INFO",
            )

        maybe_block_user_after_frauds(session["user_id"], session.get("username", ""))

    return render_page(
        "result",
        prediction_text=resultado,
        clase=clase,
        nivel_riesgo=nivel_riesgo,
        probabilidad_fraude=probabilidad_fraude,
        recomendacion=recomendacion,
        monto=monto,
        tipo_comercio=TIPO_COMERCIO_LABELS.get(tipo_comercio, tipo_comercio),
        metodo_pago=METODO_PAGO_LABELS.get(metodo_pago, metodo_pago),
        ubicacion=UBICACION_LABELS.get(stored_location, stored_location),
        frecuencia=frecuencia,
        hora=hora,
        score=score,
        selected_model_label=MODEL_LABELS.get(selected_model, "Random Forest"),
        model_comparison=model_comparison,
    )


@app.route("/simular", methods=["GET", "POST"])
@login_required
def simular_transaccion():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        requested_count = payload.get("cantidad", request.form.get("cantidad", 1))
        simulation_count = parse_simulation_count(requested_count)

        conn = get_db_connection()
        generated_items = []
        fraud_count = 0

        for _ in range(simulation_count):
            tx_data, generated_type = generate_random_transaction()
            random_geo = generate_random_geo_location(tx_data.get("ubicacion"))
            model_features = build_model_feature_values(tx_data)

            selected_result = evaluate_risk_by_model(*model_features, FIXED_MODEL_KEY)
            if selected_result["clase"] == "alto":
                fraud_count += 1

            stored_location = random_geo["geo_location_label"] or tx_data["ubicacion"]
            store_consulta_record(
                conn=conn,
                user_id=session["user_id"],
                monto=tx_data["monto"],
                tipo_comercio=tx_data["tipo_comercio"],
                metodo_pago=tx_data["metodo_pago"],
                ubicacion=stored_location,
                frecuencia=tx_data["frecuencia"],
                hora=tx_data["hora"],
                score=selected_result["score"],
                probabilidad_fraude=selected_result["probabilidad_fraude"],
                nivel_riesgo=selected_result["nivel_riesgo"],
                recomendacion=selected_result["recomendacion"],
                selected_model=FIXED_MODEL_KEY,
                geo_latitude=random_geo["geo_latitude"],
                geo_longitude=random_geo["geo_longitude"],
            )

            generated_items.append(
                {
                    "datos_transaccion": {
                        "monto": tx_data["monto"],
                        "tipo_comercio": tx_data["tipo_comercio"],
                        "metodo_pago": tx_data["metodo_pago"],
                        "ubicacion": tx_data["ubicacion"],
                        "frecuencia": tx_data["frecuencia"],
                        "hora": tx_data["hora"],
                        "geo_location_label": random_geo["geo_location_label"],
                        "geo_latitude": random_geo["geo_latitude"],
                        "geo_longitude": random_geo["geo_longitude"],
                    },
                    "resultado_modelo": {
                        "modelo": selected_result["model_label"],
                        "score": selected_result["score"],
                        "probabilidad_fraude": selected_result["probabilidad_fraude"],
                        "nivel_riesgo": selected_result["nivel_riesgo"],
                        "recomendacion": selected_result["recomendacion"],
                        "clase": selected_result["clase"],
                    },
                    "tipo_generado": generated_type,
                }
            )

        conn.commit()
        conn.close()

        last_item = generated_items[-1] if generated_items else {}
        rate = round((fraud_count / simulation_count) * 100, 2) if simulation_count else 0.0

        return jsonify(
            {
                "features_order": list(MODEL_FEATURE_ORDER),
                "resumen": {
                    "total_generadas": simulation_count,
                    "fraudes_detectados": fraud_count,
                    "tasa_fraude": rate,
                },
                "ultima": last_item,
            }
        )

    tx_data, generated_type = generate_random_transaction()
    random_geo = generate_random_geo_location(tx_data.get("ubicacion"))
    model_features = build_model_feature_values(tx_data)

    selected_result = evaluate_risk_by_model(*model_features, FIXED_MODEL_KEY)

    return jsonify(
        {
            "features_order": list(MODEL_FEATURE_ORDER),
            "datos_transaccion": {
                "monto": tx_data["monto"],
                "tipo_comercio": tx_data["tipo_comercio"],
                "metodo_pago": tx_data["metodo_pago"],
                "ubicacion": tx_data["ubicacion"],
                "frecuencia": tx_data["frecuencia"],
                "hora": tx_data["hora"],
                "geo_location_label": random_geo["geo_location_label"],
                "geo_latitude": random_geo["geo_latitude"],
                "geo_longitude": random_geo["geo_longitude"],
            },
            "resultado_modelo": {
                "modelo": selected_result["model_label"],
                "score": selected_result["score"],
                "probabilidad_fraude": selected_result["probabilidad_fraude"],
                "nivel_riesgo": selected_result["nivel_riesgo"],
                "recomendacion": selected_result["recomendacion"],
                "clase": selected_result["clase"],
            },
            "tipo_generado": generated_type,
        }
    )


@app.route("/api/transactions", methods=["POST", "OPTIONS"])
def api_create_transaction():
    if request.method == "OPTIONS":
        return ("", 204)

    payload = request.get_json(silent=True)
    cleaned_payload, validation_errors = validate_api_transaction_payload(payload)

    if validation_errors:
        return (
            jsonify(
                {
                    "mensaje": "Datos inválidos",
                    "errores": validation_errors,
                }
            ),
            400,
        )

    model_prediction, model_error = predict_api_transaction_fraud(cleaned_payload)
    if model_prediction is None:
        return (
            jsonify(
                {
                    "mensaje": "No se pudo ejecutar la predicción del modelo.",
                    "error_modelo": model_error,
                }
            ),
            500,
        )

    risk_score = compute_rule_based_risk_score(cleaned_payload)
    clasificacion = classify_risk_score(risk_score)
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    artifact_features = {
        "Genero": cleaned_payload["Genero"],
        "Edad": cleaned_payload["Edad"],
        "Ciudad": cleaned_payload["Ciudad"],
        "Tipo_Cuenta": cleaned_payload["Tipo_Cuenta"],
        "Monto_USD": cleaned_payload["Monto_USD"],
        "Tipo_Transaccion": cleaned_payload["Tipo_Transaccion"],
        "Categoria_Comercio": cleaned_payload["Categoria_Comercio"],
        "Balance_Cuenta_USD": cleaned_payload["Balance_Cuenta_USD"],
        "Dispositivo_Transaccion": cleaned_payload["Dispositivo_Transaccion"],
        "Tipo_Dispositivo": cleaned_payload["Tipo_Dispositivo"],
        "Porcentaje_Gasto": cleaned_payload["Porcentaje_Gasto"],
        "Transaccion_Grande": cleaned_payload["Transaccion_Grande"],
        "Saldo_Restante": cleaned_payload["Saldo_Restante"],
        "Compra_Riesgosa": cleaned_payload["Compra_Riesgosa"],
        "Riesgo_Edad_Monto": cleaned_payload["Riesgo_Edad_Monto"],
        "Dia_Semana": cleaned_payload["Dia_Semana"],
        "Mes": cleaned_payload["Mes"],
        "Hora": cleaned_payload["Hora"],
        "Transaccion_Nocturna": cleaned_payload["Transaccion_Nocturna"],
    }

    model_risk = evaluate_risk_by_model(
        float(cleaned_payload["Monto_USD"]),
        map_model_categoria_to_legacy_comercio(cleaned_payload["Categoria_Comercio"]),
        map_model_tipo_tx_to_legacy_metodo(cleaned_payload["Tipo_Transaccion"]),
        map_geo_label_to_legacy_ubicacion(cleaned_payload["Ciudad"]),
        cleaned_payload["Porcentaje_Gasto"],
        cleaned_payload["Hora"],
        FIXED_MODEL_KEY,
        artifact_features=artifact_features,
    )

    consulta_record = build_consulta_from_api_payload(
        cleaned_payload,
        model_risk,
        clasificacion,
        created_at,
    )

    consulta_user_id = get_api_ingestion_user_id()
    try:
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO consultas (
                user_id, monto, tipo_comercio, metodo_pago, ubicacion,
                geo_latitude, geo_longitude, frecuencia, hora, score,
                probabilidad_fraude, true_fraud_label, nivel_riesgo,
                recomendacion, modelo, resultado, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                consulta_user_id,
                consulta_record["monto"],
                consulta_record["tipo_comercio"],
                consulta_record["metodo_pago"],
                consulta_record["ubicacion"],
                consulta_record["geo_latitude"],
                consulta_record["geo_longitude"],
                consulta_record["frecuencia"],
                consulta_record["hora"],
                consulta_record["score"],
                consulta_record["probabilidad_fraude"],
                None,
                consulta_record["nivel_riesgo"],
                consulta_record["recomendacion"],
                consulta_record["modelo"],
                consulta_record["resultado"],
                consulta_record["created_at"],
            ),
        )
        conn.commit()
        conn.close()
    except sqlite3.Error:
        pass

    record_id = None
    try:
        conn = get_db_connection()
        cursor = conn.execute(
            """
            INSERT INTO api_transactions (
                genero, edad, ciudad, tipo_cuenta, monto_usd, tipo_transaccion,
                categoria_comercio, balance_cuenta_usd, dispositivo_transaccion,
                tipo_dispositivo, porcentaje_gasto, transaccion_grande,
                saldo_restante, compra_riesgosa, riesgo_edad_monto,
                dia_semana, mes, hora, transaccion_nocturna,
                model_prediction, risk_score, clasificacion
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cleaned_payload["Genero"],
                cleaned_payload["Edad"],
                cleaned_payload["Ciudad"],
                cleaned_payload["Tipo_Cuenta"],
                cleaned_payload["Monto_USD"],
                cleaned_payload["Tipo_Transaccion"],
                cleaned_payload["Categoria_Comercio"],
                cleaned_payload["Balance_Cuenta_USD"],
                cleaned_payload["Dispositivo_Transaccion"],
                cleaned_payload["Tipo_Dispositivo"],
                cleaned_payload["Porcentaje_Gasto"],
                cleaned_payload["Transaccion_Grande"],
                cleaned_payload["Saldo_Restante"],
                cleaned_payload["Compra_Riesgosa"],
                cleaned_payload["Riesgo_Edad_Monto"],
                cleaned_payload["Dia_Semana"],
                cleaned_payload["Mes"],
                cleaned_payload["Hora"],
                cleaned_payload["Transaccion_Nocturna"],
                model_prediction,
                risk_score,
                clasificacion,
            ),
        )
        record_id = cursor.lastrowid
        conn.commit()
        conn.close()
    except sqlite3.Error:
        # Keep API ingestion functional via in-memory backup even if SQLite is temporarily unavailable.
        record_id = int(datetime.now().timestamp() * 1000)

    API_TRANSACTIONS_MEMORY.insert(
        0,
        build_api_transaction_record(
            record_id=record_id,
            cleaned_payload=cleaned_payload,
            model_prediction=model_prediction,
            risk_score=risk_score,
            clasificacion=clasificacion,
            created_at=created_at,
        ),
    )

    return jsonify(
        {
            "mensaje": "Transacción procesada correctamente",
            "risk_score": risk_score,
            "clasificacion": clasificacion,
            "prediccion_modelo": "fraudulenta" if int(model_prediction) == 1 else "no_fraudulenta",
        }
    )


@app.route("/api/transactions", methods=["GET", "OPTIONS"])
def api_get_transactions():
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        conn = get_db_connection()
        rows = conn.execute(
            """
            SELECT id, genero, edad, ciudad, tipo_cuenta, monto_usd,
                   tipo_transaccion, categoria_comercio, balance_cuenta_usd,
                   dispositivo_transaccion, tipo_dispositivo, porcentaje_gasto,
                   transaccion_grande, saldo_restante, compra_riesgosa,
                   riesgo_edad_monto, dia_semana, mes, hora, transaccion_nocturna,
                   model_prediction, risk_score, clasificacion, created_at
            FROM api_transactions
            ORDER BY id DESC
            """
        ).fetchall()
        conn.close()
    except sqlite3.Error:
        rows = []

    transactions = []
    for row in rows:
        transactions.append(
            {
                "id": row["id"],
                "Genero": row["genero"],
                "Edad": row["edad"],
                "Ciudad": row["ciudad"],
                "Tipo_Cuenta": row["tipo_cuenta"],
                "Monto_USD": row["monto_usd"],
                "Tipo_Transaccion": row["tipo_transaccion"],
                "Categoria_Comercio": row["categoria_comercio"],
                "Balance_Cuenta_USD": row["balance_cuenta_usd"],
                "Dispositivo_Transaccion": row["dispositivo_transaccion"],
                "Tipo_Dispositivo": row["tipo_dispositivo"],
                "Porcentaje_Gasto": row["porcentaje_gasto"],
                "Transaccion_Grande": row["transaccion_grande"],
                "Saldo_Restante": row["saldo_restante"],
                "Compra_Riesgosa": row["compra_riesgosa"],
                "Riesgo_Edad_Monto": row["riesgo_edad_monto"],
                "Dia_Semana": row["dia_semana"],
                "Mes": row["mes"],
                "Hora": row["hora"],
                "Transaccion_Nocturna": row["transaccion_nocturna"],
                "model_prediction": row["model_prediction"],
                "risk_score": row["risk_score"],
                "clasificacion": row["clasificacion"],
                "created_at": row["created_at"],
            }
        )

    if not transactions and API_TRANSACTIONS_MEMORY:
        transactions = list(API_TRANSACTIONS_MEMORY)

    return jsonify(
        {
            "total": len(transactions),
            "transactions": transactions,
        }
    )


@app.route("/api/clear-dashboard", methods=["POST", "OPTIONS"])
@login_required
def api_clear_dashboard():
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM consultas")
        cursor.execute("DELETE FROM api_transactions")

        conn.commit()
        conn.close()

        global API_TRANSACTIONS_MEMORY
        API_TRANSACTIONS_MEMORY.clear()

        log_event(
            "dashboard_cleared",
            f"Dashboard limpiado por usuario {session.get('username', 'desconocido')}",
            session.get("username"),
            "INFO",
        )

        remote_clear = {"ok": True, "status": 0, "error": ""}
        simulator_clear_url = os.environ.get(
            "SIMULADOR_CLEAR_URL", "http://127.0.0.1:5001/api/clear-dashboard"
        )

        try:
            req = urllib_request.Request(simulator_clear_url, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("X-SIMDF-SKIP-REMOTE", "1")
            with urllib_request.urlopen(req, timeout=4) as response:
                remote_clear["status"] = int(getattr(response, "status", 200) or 200)
                remote_clear["ok"] = 200 <= remote_clear["status"] < 300
        except (urllib_error.URLError, ValueError) as exc:
            remote_clear["ok"] = False
            remote_clear["error"] = str(exc)

        return jsonify(
            {
                "mensaje": "Dashboard limpiado correctamente",
                "status": "success",
                "simulador_clear": remote_clear,
            }
        )
    except sqlite3.Error as exc:
        return (
            jsonify({"mensaje": "Error al limpiar el dashboard", "error": str(exc)}),
            500,
        )


@app.route("/api/internal/clear-dashboard", methods=["POST", "OPTIONS"])
def api_internal_clear_dashboard():
    if request.method == "OPTIONS":
        return ("", 204)

    token = request.headers.get("X-SIMDF-INTERNAL-TOKEN", "")
    if token != SIMDF_INTERNAL_TOKEN:
        return jsonify({"mensaje": "No autorizado"}), 401

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM consultas")
        cursor.execute("DELETE FROM api_transactions")

        conn.commit()
        conn.close()

        global API_TRANSACTIONS_MEMORY
        API_TRANSACTIONS_MEMORY.clear()

        return jsonify({"mensaje": "Dashboard limpiado internamente", "status": "success"})
    except sqlite3.Error as exc:
        return (
            jsonify({"mensaje": "Error al limpiar internamente", "error": str(exc)}),
            500,
        )


@app.route("/history")
@login_required
@roles_required("analista", "administrador")
def history():
    tx_id_raw = request.args.get("tx_id", "").strip()
    tx_id = int(tx_id_raw) if tx_id_raw.isdigit() else None
    date_filter = request.args.get("date_filter", "")

    consultas_formateadas = get_history_records_for_current_user(tx_id, date_filter)
    settings = get_app_settings()

    total = len(consultas_formateadas)
    fraude_alto = len([item for item in consultas_formateadas if "alto" in (item.get("nivel_riesgo", "").lower())])
    fraude_pct = round((fraude_alto / total) * 100, 1) if total else 0
    score_prom = round(sum(item.get("score", 0) for item in consultas_formateadas) / total, 2) if total else 0

    activity_log = [
        {
            "hora": (item.get("created_at", "").split(" ")[-1] if item.get("created_at") else ""),
            "usuario": item.get("username", ""),
            "resultado": item.get("resultado", ""),
        }
        for item in consultas_formateadas[:12]
    ]

    return render_page(
        "history",
        consultas=consultas_formateadas,
        activity_log=activity_log,
        tx_id=tx_id_raw,
        date_filter=date_filter,
        fraude_pct=fraude_pct,
        score_prom=score_prom,
        active_model_label=MODEL_LABELS.get(FIXED_MODEL_KEY, "LightGBM"),
    )


@app.route("/history/export/csv")
@login_required
@roles_required("analista", "administrador")
def export_history_csv():
    tx_id_raw = request.args.get("tx_id", "").strip()
    tx_id = int(tx_id_raw) if tx_id_raw.isdigit() else None
    date_filter = request.args.get("date_filter", "")
    consultas = get_history_records_for_current_user(tx_id, date_filter)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "Fecha",
            "Usuario",
            "Monto (USD)",
            "Comercio",
            "Metodo",
            "Ubicacion",
            "Frecuencia",
            "Hora",
            "Score",
            "Probabilidad",
            "Riesgo",
            "Recomendacion",
            "Resultado",
        ]
    )

    for c in consultas:
        writer.writerow(
            [
                c.get("created_at", ""),
                c.get("username", ""),
                format_currency_usd(c.get("monto", "")),
                c.get("tipo_comercio", ""),
                c.get("metodo_pago", ""),
                c.get("ubicacion", ""),
                c.get("frecuencia", ""),
                c.get("hora", ""),
                c.get("score", ""),
                f"{c.get('probabilidad_fraude', 0)}%",
                c.get("nivel_riesgo", ""),
                c.get("recomendacion", ""),
                c.get("resultado", ""),
            ]
        )

    response = make_response(output.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = (
        f"attachment; filename=simdf_historial_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    return response


@app.route("/history/export/pdf")
@login_required
@roles_required("analista", "administrador")
def export_history_pdf():
    tx_id_raw = request.args.get("tx_id", "").strip()
    tx_id = int(tx_id_raw) if tx_id_raw.isdigit() else None
    date_filter = request.args.get("date_filter", "")
    consultas = get_history_records_for_current_user(tx_id, date_filter)

    period_map = {
        "today": "Hoy",
        "7days": "Ultimos 7 dias",
        "month": "Ultimo mes",
    }
    periodo_reporte = period_map.get(date_filter, "Todo el historial")
    filtros_aplicados = []
    if tx_id:
        filtros_aplicados.append(f"ID {tx_id}")
    if date_filter:
        filtros_aplicados.append(periodo_reporte)
    filtros_texto = ", ".join(filtros_aplicados) if filtros_aplicados else "Sin filtros"

    total_consultas = len(consultas)
    riesgo_alto = len([c for c in consultas if "alto" in (c.get("nivel_riesgo", "").lower())])
    riesgo_medio = len([c for c in consultas if "medio" in (c.get("nivel_riesgo", "").lower())])
    riesgo_bajo = len([c for c in consultas if "bajo" in (c.get("nivel_riesgo", "").lower())])
    fraude_detectado = riesgo_alto
    transacciones_normales = max(0, total_consultas - fraude_detectado)
    tasa_fraude = (fraude_detectado / total_consultas * 100) if total_consultas else 0
    riesgo_promedio = (
        sum(float(c.get("probabilidad_fraude", 0) or 0) for c in consultas) / total_consultas
        if total_consultas
        else 0
    )

    model_catalog = get_model_catalog()
    active_model_metrics = model_catalog.get(FIXED_MODEL_KEY, {})
    precision_modelo = active_model_metrics.get("accuracy", 92.8)
    recall_modelo = active_model_metrics.get("recall", 91.7)
    f1_modelo = active_model_metrics.get("f1_score", 91.1)
    auc_modelo = active_model_metrics.get("auc_roc", 0.96)

    def parse_hour_value(raw_hour):
        hour_text = str(raw_hour or "")
        if ":" in hour_text:
            try:
                return int(hour_text.split(":", 1)[0])
            except (TypeError, ValueError):
                return None
        try:
            return int(hour_text)
        except (TypeError, ValueError):
            return None

    factor_monto_alto = 0
    factor_ubicacion_internacional = 0
    factor_hora_inusual = 0
    factor_frecuencia_alta = 0
    hourly_fraud = [0] * 24
    commerce_counter = {}
    table_rows = []
    alerts_rows = []

    for item in consultas:
        monto_val = float(item.get("monto", 0) or 0)
        if monto_val >= 900:
            factor_monto_alto += 1

        ubicacion_lower = str(item.get("ubicacion", "") or "").lower()
        if "internacional" in ubicacion_lower:
            factor_ubicacion_internacional += 1

        parsed_hour = parse_hour_value(item.get("hora", ""))
        if parsed_hour is not None and (parsed_hour <= 5 or parsed_hour >= 23):
            factor_hora_inusual += 1

        frecuencia_val = int(item.get("frecuencia", 0) or 0)
        if frecuencia_val >= 8:
            factor_frecuencia_alta += 1

        comercio_key = str(item.get("tipo_comercio", "") or "Sin dato")
        commerce_counter[comercio_key] = commerce_counter.get(comercio_key, 0) + 1

        hora = item.get("created_at", "").split(" ")[-1] if item.get("created_at") else ""
        user = str(item.get("username", ""))[:14]
        riesgo = "Alto"
        riesgo_lower = (item.get("nivel_riesgo", "") or "").lower()
        if "medio" in riesgo_lower:
            riesgo = "Medio"
        elif "bajo" in riesgo_lower:
            riesgo = "Bajo"
        prob = f"{int(item.get('probabilidad_fraude', 0) or 0)}%"
        accion = str(item.get("recomendacion", ""))[:34]
        monto_fmt = format_currency_usd(item.get("monto", 0))
        resultado = str(item.get("resultado", ""))[:30]
        tx_id_value = item.get("id", "")
        tx_code = f"TX{int(tx_id_value):04d}" if str(tx_id_value).isdigit() else f"TX-{tx_id_value}"
        if parsed_hour is not None and "alto" in riesgo_lower:
            hourly_fraud[parsed_hour] += 1

        if "alto" in riesgo_lower and len(alerts_rows) < 10:
            alerts_rows.append(
                {
                    "tx": tx_code,
                    "hora": hora,
                    "usuario": user,
                    "prob": prob,
                    "riesgo": riesgo,
                    "accion": accion or "Revisar manualmente",
                }
            )

        table_rows.append(
            {
                "id": tx_code,
                "fecha_hora": item.get("created_at", ""),
                "hora": hora,
                "usuario": user,
                "monto": monto_fmt,
                "riesgo": riesgo,
                "prob": prob,
                "resultado": resultado,
                "accion": accion,
            }
        )

    denominator = total_consultas if total_consultas else 1
    top_factors = [
        {
            "name": "Monto alto",
            "percent": (factor_monto_alto / denominator) * 100,
        },
        {
            "name": "Ubicacion internacional",
            "percent": (factor_ubicacion_internacional / denominator) * 100,
        },
        {
            "name": "Hora inusual",
            "percent": (factor_hora_inusual / denominator) * 100,
        },
        {
            "name": "Frecuencia alta",
            "percent": (factor_frecuencia_alta / denominator) * 100,
        },
    ]
    top_factors.sort(key=lambda x: x["percent"], reverse=True)

    commerce_sorted = sorted(commerce_counter.items(), key=lambda x: x[1], reverse=True)
    commerce_labels = [item[0][:14] for item in commerce_sorted[:6]] or ["Sin datos"]
    commerce_values = [item[1] for item in commerce_sorted[:6]] or [0]
    hourly_labels = [f"{h:02d}" for h in range(24)]

    report_data = {
        "usuario_solicitante": session.get("username", ""),
        "fecha_reporte": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "nombre_sistema": "Sistema Interno de Monitoreo y Deteccion de Fraude",
        "nombre_reporte": "Reporte Ejecutivo de Riesgo Transaccional",
        "modelo_utilizado": "LightGBM",
        "periodo_reporte": periodo_reporte,
        "filtros_aplicados": filtros_texto,
        "total_consultas": total_consultas,
        "fraude_detectado": fraude_detectado,
        "transacciones_normales": transacciones_normales,
        "riesgo_promedio": riesgo_promedio,
        "alertas_generadas": len(alerts_rows),
        "riesgo_alto": riesgo_alto,
        "riesgo_medio": riesgo_medio,
        "riesgo_bajo": riesgo_bajo,
        "precision_modelo": precision_modelo,
        "recall_modelo": recall_modelo,
        "f1_modelo": f1_modelo,
        "auc_modelo": auc_modelo,
        "tasa_fraude": tasa_fraude,
        "top_factors": top_factors,
        "hourly_fraud": hourly_fraud,
        "hourly_labels": hourly_labels,
        "commerce_labels": commerce_labels,
        "commerce_values": commerce_values,
        "alerts_rows": alerts_rows,
        "table_rows": table_rows,
    }

    pdf_data = build_professional_pdf(report_data)
    file_name = f"simdf_reporte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

    return send_file(
        BytesIO(pdf_data),
        as_attachment=True,
        download_name=file_name,
        mimetype="application/pdf",
    )


@app.route("/configuracion-analista", methods=["GET", "POST"])
@login_required
@roles_required("analista", "administrador")
def analyst_config_page():
    status_message = None
    error = None

    if request.method == "POST":
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        phone = request.form.get("phone", "").strip()
        theme_preference = request.form.get("theme_preference", "light").strip()
        interface_size = request.form.get("interface_size", "compact").strip()
        notif_high_risk = 1 if request.form.get("notif_high_risk") == "1" else 0
        notif_critical_alerts = 1 if request.form.get("notif_critical_alerts") == "1" else 0
        notif_analysis_complete = 1 if request.form.get("notif_analysis_complete") == "1" else 0
        notif_new_transactions = 1 if request.form.get("notif_new_transactions") == "1" else 0
        notif_user_activity = 1 if request.form.get("notif_user_activity") == "1" else 0
        notif_in_app = 1
        notif_frequency = request.form.get("notif_frequency", "immediate").strip()
        current_profile_photo_url = request.form.get("current_profile_photo_url", "").strip()
        profile_photo_url = current_profile_photo_url
        remove_profile_photo = request.form.get("remove_profile_photo") == "1"
        profile_photo_file = request.files.get("profile_photo_file")

        if theme_preference not in THEME_LABELS:
            theme_preference = "light"
        if interface_size not in INTERFACE_SIZE_LABELS:
            interface_size = "compact"
        if notif_frequency not in NOTIFICATION_FREQUENCY_LABELS:
            notif_frequency = "immediate"

        if email and not is_valid_email(email):
            error = "Debes registrar un correo válido."

        if remove_profile_photo:
            profile_photo_url = ""

        if not error and profile_photo_file and profile_photo_file.filename:
            if not is_allowed_profile_image(profile_photo_file.filename):
                error = "Formato de imagen no permitido. Usa PNG, JPG, JPEG, WEBP o GIF."
            else:
                os.makedirs(PROFILE_UPLOAD_DIR, exist_ok=True)
                saved_filename = build_profile_image_filename(session["user_id"], profile_photo_file.filename)
                save_path = os.path.join(PROFILE_UPLOAD_DIR, saved_filename)
                profile_photo_file.save(save_path)
                profile_photo_url = url_for("static", filename=f"uploads/profiles/{saved_filename}")

        if not error:
            conn = get_db_connection()
            conn.execute(
                """
                UPDATE users
                SET full_name = ?, email = ?, phone = ?, profile_photo_url = ?,
                    theme_preference = ?, interface_size = ?,
                    notif_high_risk = ?, notif_critical_alerts = ?, notif_analysis_complete = ?,
                    notif_new_transactions = ?, notif_user_activity = ?, notif_in_app = ?, notif_frequency = ?
                WHERE id = ?
                """,
                (
                    full_name,
                    email,
                    phone,
                    profile_photo_url,
                    theme_preference,
                    interface_size,
                    notif_high_risk,
                    notif_critical_alerts,
                    notif_analysis_complete,
                    notif_new_transactions,
                    notif_user_activity,
                    notif_in_app,
                    notif_frequency,
                    session["user_id"],
                ),
            )
            conn.commit()
            conn.close()
            log_event("PROFILE", f"Configuración de analista actualizada por {session.get('username')}")
            status_message = "Configuración del analista actualizada correctamente."

    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT username, role, full_name, email, phone, profile_photo_url,
             theme_preference, interface_size,
             notif_high_risk, notif_critical_alerts, notif_analysis_complete,
             notif_new_transactions, notif_user_activity, notif_in_app, notif_frequency,
             created_at
        FROM users
        WHERE id = ?
        """,
        (session["user_id"],),
    ).fetchone()
    conn.close()

    profile = dict(row) if row else {}
    if not profile.get("theme_preference") or profile.get("theme_preference") not in THEME_LABELS:
        profile["theme_preference"] = "light"
    if not profile.get("interface_size") or profile.get("interface_size") not in INTERFACE_SIZE_LABELS:
        profile["interface_size"] = "compact"
    profile["notif_high_risk"] = 1 if int(profile.get("notif_high_risk", 1) or 0) == 1 else 0
    profile["notif_critical_alerts"] = 1 if int(profile.get("notif_critical_alerts", 1) or 0) == 1 else 0
    profile["notif_analysis_complete"] = 1 if int(profile.get("notif_analysis_complete", 1) or 0) == 1 else 0
    profile["notif_new_transactions"] = 1 if int(profile.get("notif_new_transactions", 1) or 0) == 1 else 0
    profile["notif_user_activity"] = 1 if int(profile.get("notif_user_activity", 0) or 0) == 1 else 0
    profile["notif_in_app"] = 1
    if not profile.get("notif_frequency") or profile.get("notif_frequency") not in NOTIFICATION_FREQUENCY_LABELS:
        profile["notif_frequency"] = "immediate"
    if profile.get("created_at"):
        profile["created_at_fmt"] = format_db_datetime(profile.get("created_at"))
    else:
        profile["created_at_fmt"] = ""

    return render_page(
        "analyst_config",
        profile=profile,
        theme_choices=THEME_LABELS,
        interface_size_choices=INTERFACE_SIZE_LABELS,
        status_message=status_message,
        error=error,
    )


@app.route("/configuracion-analista/notificaciones", methods=["GET", "POST"])
@login_required
@roles_required("analista", "administrador")
def analyst_notifications_page():
    status_message = None

    if request.method == "POST":
        notif_high_risk = 1 if request.form.get("notif_high_risk") == "1" else 0
        notif_critical_alerts = 1 if request.form.get("notif_critical_alerts") == "1" else 0
        notif_analysis_complete = 1 if request.form.get("notif_analysis_complete") == "1" else 0
        notif_new_transactions = 1 if request.form.get("notif_new_transactions") == "1" else 0
        notif_user_activity = 1 if request.form.get("notif_user_activity") == "1" else 0
        notif_in_app = 1
        notif_frequency = request.form.get("notif_frequency", "immediate").strip()

        if notif_frequency not in NOTIFICATION_FREQUENCY_LABELS:
            notif_frequency = "immediate"

        conn = get_db_connection()
        conn.execute(
            """
            UPDATE users
            SET notif_high_risk = ?, notif_critical_alerts = ?, notif_analysis_complete = ?,
                notif_new_transactions = ?, notif_user_activity = ?, notif_in_app = ?, notif_frequency = ?
            WHERE id = ?
            """,
            (
                notif_high_risk,
                notif_critical_alerts,
                notif_analysis_complete,
                notif_new_transactions,
                notif_user_activity,
                notif_in_app,
                notif_frequency,
                session["user_id"],
            ),
        )
        conn.commit()
        conn.close()

        log_event("PROFILE", f"Notificaciones de analista actualizadas por {session.get('username')}")
        status_message = "Notificaciones actualizadas correctamente."

    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT username, role,
               notif_high_risk, notif_critical_alerts, notif_analysis_complete,
               notif_new_transactions, notif_user_activity, notif_in_app, notif_frequency
        FROM users
        WHERE id = ?
        """,
        (session["user_id"],),
    ).fetchone()
    conn.close()

    profile = dict(row) if row else {}
    profile["notif_high_risk"] = 1 if int(profile.get("notif_high_risk", 1) or 0) == 1 else 0
    profile["notif_critical_alerts"] = 1 if int(profile.get("notif_critical_alerts", 1) or 0) == 1 else 0
    profile["notif_analysis_complete"] = 1 if int(profile.get("notif_analysis_complete", 1) or 0) == 1 else 0
    profile["notif_new_transactions"] = 1 if int(profile.get("notif_new_transactions", 1) or 0) == 1 else 0
    profile["notif_user_activity"] = 1 if int(profile.get("notif_user_activity", 0) or 0) == 1 else 0
    profile["notif_in_app"] = 1
    if not profile.get("notif_frequency") or profile.get("notif_frequency") not in NOTIFICATION_FREQUENCY_LABELS:
        profile["notif_frequency"] = "immediate"

    return render_page(
        "analyst_notifications",
        profile=profile,
        notification_frequency_choices=NOTIFICATION_FREQUENCY_LABELS,
        status_message=status_message,
    )


@app.route("/configuracion-analista/seguridad", methods=["GET", "POST"])
@login_required
@roles_required("analista", "administrador")
def analyst_security_page():
    status_message = None
    error = None
    notice = None

    if request.method == "POST":
        action = request.form.get("action", "update_security_preferences").strip()

        if action == "update_password":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")

            if len(new_password) < 8:
                error = "La nueva contraseña debe tener al menos 8 caracteres."
            elif new_password != confirm_password:
                error = "La confirmación de contraseña no coincide."
            else:
                conn = get_db_connection()
                user_row = conn.execute(
                    "SELECT password_hash FROM users WHERE id = ?",
                    (session["user_id"],),
                ).fetchone()

                if not user_row or not check_password_hash(user_row["password_hash"], current_password):
                    error = "La contraseña actual es incorrecta."
                    conn.close()
                else:
                    conn.execute(
                        "UPDATE users SET password_hash = ? WHERE id = ?",
                        (generate_password_hash(new_password), session["user_id"]),
                    )
                    conn.commit()
                    conn.close()
                    log_event("SECURITY", f"Contraseña actualizada por {session.get('username')}")
                    status_message = "Contraseña actualizada correctamente."

        elif action == "close_sessions" or action == "close_other_session":
            conn = get_db_connection()
            conn.execute(
                "UPDATE users SET session_version = COALESCE(session_version, 0) + 1 WHERE id = ?",
                (session["user_id"],),
            )
            row = conn.execute(
                "SELECT session_version FROM users WHERE id = ?",
                (session["user_id"],),
            ).fetchone()
            conn.commit()
            conn.close()

            session["session_version"] = (row["session_version"] if row else 0)
            log_event("SECURITY", f"Sesiones activas cerradas por {session.get('username')}", severity="WARN")
            notice = "Se cerraron las demás sesiones activas de esta cuenta."

        elif action == "update_security_preferences":
            security_two_factor = 1 if request.form.get("security_two_factor") == "1" else 0
            security_notify_login = 1 if request.form.get("security_notify_login") == "1" else 0
            security_notify_failed = 1 if request.form.get("security_notify_failed") == "1" else 0

            conn = get_db_connection()
            conn.execute(
                """
                UPDATE users
                SET security_two_factor = ?, security_notify_login = ?, security_notify_failed = ?
                WHERE id = ?
                """,
                (
                    security_two_factor,
                    security_notify_login,
                    security_notify_failed,
                    session["user_id"],
                ),
            )
            conn.commit()
            conn.close()
            log_event("SECURITY", f"Preferencias de seguridad actualizadas por {session.get('username')}")
            status_message = "Preferencias de seguridad actualizadas correctamente."

    conn = get_db_connection()
    profile_row = conn.execute(
        """
        SELECT username, role,
               security_two_factor, security_notify_login, security_notify_failed
        FROM users
        WHERE id = ?
        """,
        (session["user_id"],),
    ).fetchone()

    attempts_rows = conn.execute(
        """
        SELECT created_at, ip_address, user_agent
        FROM login_attempts
        WHERE username = ?
        ORDER BY created_at DESC
        LIMIT 8
        """,
        (session.get("username", ""),),
    ).fetchall()
    conn.close()

    profile = dict(profile_row) if profile_row else {}
    profile["security_two_factor"] = 1 if int(profile.get("security_two_factor", 0) or 0) == 1 else 0
    profile["security_notify_login"] = 1 if int(profile.get("security_notify_login", 1) or 0) == 1 else 0
    profile["security_notify_failed"] = 1 if int(profile.get("security_notify_failed", 1) or 0) == 1 else 0

    security_activity = []
    for row in attempts_rows:
        security_activity.append(
            {
                "created_at": format_db_datetime(row["created_at"]),
                "device": get_client_device_label(row["user_agent"]),
                "ip_address": row["ip_address"],
            }
        )

    active_sessions = [
        {
            "label": "Este dispositivo",
            "detail": f"{get_client_device_label(request.headers.get('User-Agent', ''))} - Sesión actual",
            "is_current": True,
        },
        {
            "label": "Otro dispositivo",
            "detail": "Sesión web registrada recientemente",
            "is_current": False,
        },
    ]

    return render_page(
        "analyst_security",
        profile=profile,
        security_activity=security_activity,
        active_sessions=active_sessions,
        status_message=status_message,
        error=error,
        notice=notice,
    )


@app.route("/configuracion", methods=["GET", "POST"])
@login_required
@roles_required("administrador")
def system_config():
    settings = get_app_settings()
    status_message = None

    if request.method == "POST":
        language = request.form.get("language", "es")
        if language not in LANGUAGE_LABELS:
            language = "es"

        theme = request.form.get("theme", "light")
        if theme not in THEME_LABELS:
            theme = "light"

        payload = {
            "active_model": FIXED_MODEL_KEY,
            "fraud_block_threshold": request.form.get("fraud_block_threshold", "4"),
            "block_minutes": request.form.get("block_minutes", "30"),
            "alert_email_enabled": "1" if request.form.get("alert_email_enabled") == "on" else "0",
            "alert_recipient": request.form.get("alert_recipient", "fraude@simdf.local").strip() or "fraude@simdf.local",
            "language": language,
            "theme": theme,
        }
        save_app_settings(payload)
        log_event("CONFIG", "Parámetros del sistema actualizados", severity="INFO")
        settings = get_app_settings()
        status_message = "Configuración actualizada correctamente."

    return render_page(
        "system_config",
        settings=settings,
        model_choices=MODEL_LABELS,
        language_choices=LANGUAGE_LABELS,
        theme_choices=THEME_LABELS,
        status_message=status_message,
    )


@app.route("/logs")
@login_required
@roles_required("administrador")
def system_logs_page():
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT id, event_type, severity, message, actor_username, created_at
        FROM system_logs
        ORDER BY created_at DESC
        LIMIT 200
        """
    ).fetchall()
    conn.close()

    logs = [dict(row) for row in rows]
    for item in logs:
        item["created_at"] = format_db_datetime(item.get("created_at"))

    return render_page("system_logs", logs=logs)


@app.route("/seguridad-sistema", methods=["GET", "POST"])
@login_required
@roles_required("administrador")
def system_security_page():
    notice = None
    error = None

    if request.method == "POST":
        action = request.form.get("action", "").strip()
        ip_address = request.form.get("ip_address", "").strip()

        if action == "block_ip":
            duration_raw = request.form.get("duration_minutes", "60").strip()
            duration = int(duration_raw) if duration_raw.isdigit() else 60
            reason = request.form.get("reason", "Bloqueo manual por actividad sospechosa").strip()

            if not ip_address:
                error = "Debes indicar una IP para bloquear."
            else:
                blocked_until = block_ip_address(
                    ip_address,
                    reason or "Bloqueo manual por actividad sospechosa",
                    duration,
                    session.get("username", "admin"),
                )
                notice = f"IP {ip_address} bloqueada hasta {format_db_datetime(blocked_until)}"
                log_event(
                    "SECURITY",
                    f"IP {ip_address} bloqueada manualmente por {session.get('username')}. Motivo: {reason}",
                    severity="WARN",
                )

        elif action == "unblock_ip":
            if not ip_address:
                error = "Debes indicar una IP para desbloquear."
            else:
                conn = get_db_connection()
                conn.execute("DELETE FROM blocked_ips WHERE ip_address = ?", (ip_address,))
                conn.commit()
                conn.close()
                notice = f"IP {ip_address} desbloqueada correctamente."
                log_event(
                    "SECURITY",
                    f"IP {ip_address} desbloqueada manualmente por {session.get('username')}",
                    severity="INFO",
                )

    conn = get_db_connection()
    attempts_rows = conn.execute(
        """
        SELECT username, ip_address, success, failure_reason, created_at
        FROM login_attempts
        ORDER BY created_at DESC
        LIMIT 140
        """
    ).fetchall()

    blocked_rows = conn.execute(
        """
        SELECT ip_address, reason, blocked_until, created_by, created_at,
               CASE WHEN blocked_until > datetime('now') THEN 1 ELSE 0 END AS active
        FROM blocked_ips
        ORDER BY blocked_until DESC
        LIMIT 120
        """
    ).fetchall()

    monitor_rows = conn.execute(
        """
        SELECT event_type, severity, actor_username, message, created_at
        FROM system_logs
        WHERE event_type IN ('AUTH', 'SECURITY')
        ORDER BY created_at DESC
        LIMIT 140
        """
    ).fetchall()

    total_attempts_24h = conn.execute(
        "SELECT COUNT(*) AS total FROM login_attempts WHERE created_at >= datetime('now', '-1 day')"
    ).fetchone()["total"]
    failed_attempts_24h = conn.execute(
        "SELECT COUNT(*) AS total FROM login_attempts WHERE success = 0 AND created_at >= datetime('now', '-1 day')"
    ).fetchone()["total"]
    active_blocked_ips = conn.execute(
        "SELECT COUNT(*) AS total FROM blocked_ips WHERE blocked_until > datetime('now')"
    ).fetchone()["total"]
    unique_ips_24h = conn.execute(
        "SELECT COUNT(DISTINCT ip_address) AS total FROM login_attempts WHERE created_at >= datetime('now', '-1 day')"
    ).fetchone()["total"]
    conn.close()

    attempts = []
    for row in attempts_rows:
        attempts.append(
            {
                "username": row["username"] or "desconocido",
                "ip_address": row["ip_address"],
                "success": bool(row["success"]),
                "failure_reason": row["failure_reason"] or "",
                "created_at": format_db_datetime(row["created_at"]),
            }
        )

    blocked_ips = []
    for row in blocked_rows:
        blocked_ips.append(
            {
                "ip_address": row["ip_address"],
                "reason": row["reason"],
                "blocked_until": format_db_datetime(row["blocked_until"]),
                "created_by": row["created_by"],
                "created_at": format_db_datetime(row["created_at"]),
                "active": bool(row["active"]),
            }
        )

    access_monitor = []
    for row in monitor_rows:
        access_monitor.append(
            {
                "event_type": row["event_type"],
                "severity": row["severity"],
                "actor_username": row["actor_username"],
                "message": row["message"],
                "created_at": format_db_datetime(row["created_at"]),
            }
        )

    security_stats = {
        "total_attempts_24h": total_attempts_24h or 0,
        "failed_attempts_24h": failed_attempts_24h or 0,
        "active_blocked_ips": active_blocked_ips or 0,
        "unique_ips_24h": unique_ips_24h or 0,
    }

    return render_page(
        "system_security",
        security_stats=security_stats,
        attempts=attempts,
        blocked_ips=blocked_ips,
        access_monitor=access_monitor,
        notice=notice,
        error=error,
    )


@app.route("/usuarios", methods=["GET", "POST"])
@login_required
@roles_required("administrador")
def user_management():
    notice = None

    if request.method == "POST":
        action = request.form.get("action", "")
        user_id = request.form.get("user_id", "")

        if user_id.isdigit():
            user_id_int = int(user_id)
            conn = get_db_connection()

            target_user = conn.execute(
                "SELECT id, username, role, blocked_until FROM users WHERE id = ?",
                (user_id_int,),
            ).fetchone()

            if target_user:
                if action == "change_role":
                    new_role = request.form.get("new_role", "analista")
                    if new_role in ("analista", "administrador") and target_user["id"] != session.get("user_id"):
                        conn.execute("UPDATE users SET role = ? WHERE id = ?", (new_role, user_id_int))
                        notice = f"Rol actualizado para {target_user['username']}"
                        log_event("USERS", f"Rol de {target_user['username']} actualizado a {new_role}")

                elif action == "unblock":
                    conn.execute("UPDATE users SET blocked_until = NULL WHERE id = ?", (user_id_int,))
                    notice = f"Usuario {target_user['username']} desbloqueado"
                    log_event("USERS", f"Usuario {target_user['username']} desbloqueado manualmente", severity="WARN")

                elif action == "block_30m":
                    until = (datetime.now() + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
                    conn.execute("UPDATE users SET blocked_until = ? WHERE id = ?", (until, user_id_int))
                    notice = f"Usuario {target_user['username']} bloqueado por 30 minutos"
                    log_event("USERS", f"Usuario {target_user['username']} bloqueado manualmente por 30 minutos", severity="WARN")

                conn.commit()

            conn.close()

    conn = get_db_connection()
    users = conn.execute(
        """
        SELECT u.id, u.username, u.role, u.blocked_until,
               COUNT(c.id) AS total_consultas,
               SUM(CASE WHEN c.probabilidad_fraude >= 70 THEN 1 ELSE 0 END) AS fraudes_altos
        FROM users u
        LEFT JOIN consultas c ON c.user_id = u.id
        GROUP BY u.id, u.username, u.role, u.blocked_until
        ORDER BY u.username ASC
        """
    ).fetchall()
    conn.close()

    users_data = []
    for row in users:
        item = dict(row)
        item["blocked_until_fmt"] = format_db_datetime(item.get("blocked_until")) if item.get("blocked_until") else ""
        item["fraudes_altos"] = item.get("fraudes_altos") or 0
        item["total_consultas"] = item.get("total_consultas") or 0
        users_data.append(item)

    return render_page("user_management", users=users_data, notice=notice)


@app.route("/gestion-modelo", methods=["GET", "POST"])
@login_required
@roles_required("administrador")
def model_management():
    settings = get_app_settings()
    notice = "Modelo fijo en LightGBM."

    if request.method == "POST":
        save_app_settings({"active_model": FIXED_MODEL_KEY})
        settings = get_app_settings()
        notice = "Modelo fijado en LightGBM."
        log_event("MODEL", "Intento de actualización de modelo redirigido a LightGBM fijo", severity="INFO")

    model_catalog = get_model_catalog()
    active_model = FIXED_MODEL_KEY

    ranked_models = sorted(
        model_catalog.items(),
        key=lambda item: (
            float(item[1].get("auc_roc", 0.0) or 0.0),
            float(item[1].get("accuracy", 0.0) or 0.0),
        ),
        reverse=True,
    )

    model_ranking = []
    for index, (model_key, metrics) in enumerate(ranked_models):
        model_ranking.append(
            {
                "key": model_key,
                "label": MODEL_LABELS.get(model_key, model_key),
                "auc_roc": metrics.get("auc_roc", 0.0),
                "accuracy": metrics.get("accuracy", 0.0),
                "is_best": index == 0,
                "is_active": model_key == active_model,
            }
        )

    best_model = model_ranking[0] if model_ranking else None
    if best_model:
        comparison_conclusion = (
            f"El modelo {best_model['label']} fue seleccionado por presentar el mejor desempeño en AUC y accuracy."
        )
    else:
        comparison_conclusion = "No hay métricas disponibles para generar una conclusión automática."

    return render_page(
        "model_management",
        settings=settings,
        model_choices=MODEL_LABELS,
        model_catalog=model_catalog,
        model_ranking=model_ranking,
        comparison_conclusion=comparison_conclusion,
        active_model=active_model,
        notice=notice,
    )


@app.route("/gestion-datos", methods=["GET", "POST"])
@login_required
@roles_required("administrador")
def data_management():
    notice = None
    error = None

    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "upload_dataset":
            dataset_file = request.files.get("dataset_file")
            model_key = FIXED_MODEL_KEY

            if not dataset_file or not dataset_file.filename:
                error = "Selecciona un archivo CSV para cargar."
            elif not dataset_file.filename.lower().endswith(".csv"):
                error = "Formato no soportado. Sube un archivo .csv"
            else:
                try:
                    file_bytes = dataset_file.stream.read()
                    csv_text = None
                    for encoding in ("utf-8-sig", "latin-1"):
                        try:
                            csv_text = file_bytes.decode(encoding)
                            break
                        except UnicodeDecodeError:
                            continue

                    if csv_text is None:
                        error = "No se pudo leer el archivo. Usa codificación UTF-8 o Latin-1."
                        raise ValueError("encoding_error")

                    reader = csv.DictReader(StringIO(csv_text))

                    if not reader.fieldnames:
                        error = "El archivo CSV no contiene encabezados válidos."
                        raise ValueError("missing_headers")

                    monto_col = find_csv_column(reader.fieldnames, ["monto", "monto_usd", "amount", "amount_usd"])
                    frecuencia_col = find_csv_column(reader.fieldnames, ["frecuencia", "frequency", "frecuencia_reciente"])
                    fraude_col = find_csv_column(
                        reader.fieldnames,
                        [
                            "es_fraude",
                            "fraude",
                            "is_fraud",
                            "fraud",
                            "fraud_label",
                            "fraud_flag",
                            "is_fraudulent",
                            "label",
                            "target",
                            "class",
                            "clase",
                            "resultado_fraude",
                        ],
                    )
                    hora_col = find_csv_column(reader.fieldnames, ["hora", "hora_transaccion", "hour", "transaction_hour"])
                    tipo_col = find_csv_column(reader.fieldnames, ["tipo_comercio", "categoria_comercio", "categoria", "commerce_category"])
                    metodo_col = find_csv_column(reader.fieldnames, ["metodo_pago", "tipo_transaccion", "metodo", "payment_method", "transaction_type"])
                    ubicacion_col = find_csv_column(reader.fieldnames, ["ubicacion", "ciudad", "location", "city", "pais", "country"])

                    missing = []
                    if not monto_col:
                        missing.append("monto/monto_usd")
                    if not hora_col:
                        missing.append("hora/hora_transaccion")
                    if not tipo_col:
                        missing.append("tipo_comercio/categoria_comercio")
                    if not metodo_col:
                        missing.append("metodo_pago/tipo_transaccion")
                    if not ubicacion_col:
                        missing.append("ubicacion/ciudad")
                    if not frecuencia_col and not fraude_col:
                        missing.append("frecuencia o es_fraude")

                    if missing:
                        error = "Faltan columnas requeridas o equivalentes: " + ", ".join(missing)
                    else:
                        inserted = 0
                        skipped = 0
                        conn = get_db_connection()

                        for row in reader:
                            try:
                                if not any(str(value or "").strip() for value in row.values()):
                                    continue

                                monto = float(str(row.get(monto_col, "")).strip().replace(",", "."))
                                hora = parse_dataset_hour(row.get(hora_col, ""))
                                tipo_comercio = map_dataset_tipo_comercio(row.get(tipo_col, ""))
                                metodo_pago = map_dataset_metodo_pago(row.get(metodo_col, ""))
                                scoring_ubicacion, stored_ubicacion = map_dataset_ubicacion(row.get(ubicacion_col, ""))

                                if frecuencia_col:
                                    frecuencia = float(str(row.get(frecuencia_col, "")).strip().replace(",", "."))
                                else:
                                    fraude_label = parse_fraud_label(row.get(fraude_col, "0"))
                                    frecuencia = 9.0 if fraude_label == 1 else 3.0

                                frecuencia = max(0.0, frecuencia)

                                result = evaluate_risk_by_model(
                                    monto,
                                    tipo_comercio,
                                    metodo_pago,
                                    scoring_ubicacion,
                                    frecuencia,
                                    hora,
                                    model_key,
                                )
                                icon = {"alto": "🔴", "medio": "🟡", "bajo": "🟢"}.get(result["clase"], "⚪")
                                resultado = f"{icon} {result['nivel_riesgo']} · {result['recomendacion']}"

                                true_fraud_label = None
                                if fraude_col:
                                    true_fraud_label = parse_fraud_label(row.get(fraude_col, ""))

                                conn.execute(
                                    """
                                    INSERT INTO consultas (
                                        user_id, monto, tipo_comercio, metodo_pago, ubicacion, frecuencia, hora,
                                        score, probabilidad_fraude, true_fraud_label, nivel_riesgo, recomendacion, modelo, resultado
                                    )
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        session["user_id"],
                                        monto,
                                        tipo_comercio,
                                        metodo_pago,
                                        stored_ubicacion,
                                        frecuencia,
                                        hora,
                                        result["score"],
                                        result["probabilidad_fraude"],
                                        true_fraud_label,
                                        result["nivel_riesgo"],
                                        result["recomendacion"],
                                        model_key,
                                        resultado,
                                    ),
                                )
                                inserted += 1
                            except (TypeError, ValueError):
                                skipped += 1

                        conn.commit()

                        conn.execute(
                            """
                            INSERT INTO dataset_uploads (
                                file_name, target_table, records_imported, records_skipped, uploaded_by
                            )
                            VALUES (?, 'consultas', ?, ?, ?)
                            """,
                            (
                                dataset_file.filename,
                                inserted,
                                skipped,
                                session.get("username", "admin"),
                            ),
                        )
                        conn.commit()

                        conn.close()

                        if inserted:
                            log_event(
                                "DATA",
                                f"Dataset cargado por {session.get('username')}: {inserted} registros, {skipped} omitidos.",
                                severity="INFO",
                            )
                            notice = (
                                f"Dataset montado en tabla consultas: {inserted} registros importados "
                                f"y {skipped} omitidos."
                            )
                        else:
                            error = "No se importaron registros. Revisa el formato y contenido del CSV."
                except UnicodeDecodeError:
                    error = "No se pudo leer el archivo. Usa codificación UTF-8 o Latin-1."
                except ValueError:
                    if not error:
                        error = "No se pudo procesar el archivo CSV. Verifica los encabezados y el contenido."

        elif action == "clean_records":
            target_table = request.form.get("target_table", "consultas")
            days_raw = request.form.get("older_than_days", "0").strip()
            days = int(days_raw) if days_raw.isdigit() else 0

            if target_table not in {"consultas", "logs", "all"}:
                target_table = "consultas"

            tables = []
            if target_table in ("consultas", "all"):
                tables.append("consultas")
            if target_table in ("logs", "all"):
                tables.append("system_logs")

            conn = get_db_connection()
            deleted_total = 0
            for table_name in tables:
                if days > 0:
                    cursor = conn.execute(
                        f"DELETE FROM {table_name} WHERE created_at < datetime('now', ?)",
                        (f"-{days} day",),
                    )
                else:
                    cursor = conn.execute(f"DELETE FROM {table_name}")
                deleted_total += max(0, cursor.rowcount)

            conn.commit()
            conn.close()

            scope_label = {
                "consultas": "consultas",
                "logs": "logs",
                "all": "consultas y logs",
            }.get(target_table, "consultas")
            period_label = f" mayores a {days} días" if days > 0 else ""
            notice = f"Limpieza completada: {deleted_total} registros eliminados en {scope_label}{period_label}."
            log_event(
                "DATA",
                f"Limpieza de {scope_label}{period_label} ejecutada por {session.get('username')} ({deleted_total} registros).",
                severity="WARN",
            )

    conn = get_db_connection()
    total_consultas = conn.execute("SELECT COUNT(*) AS total FROM consultas").fetchone()["total"]
    fraudes_altos = conn.execute(
        "SELECT COUNT(*) AS total FROM consultas WHERE probabilidad_fraude >= 70"
    ).fetchone()["total"]
    total_logs = conn.execute("SELECT COUNT(*) AS total FROM system_logs").fetchone()["total"]
    last_data_event = conn.execute(
        """
        SELECT message, created_at
        FROM system_logs
        WHERE event_type = 'DATA'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    last_dataset_upload = conn.execute(
        """
        SELECT file_name, target_table, records_imported, records_skipped, uploaded_by, uploaded_at
        FROM dataset_uploads
        ORDER BY uploaded_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    dataset_upload_history_rows = conn.execute(
        """
        SELECT file_name, target_table, records_imported, records_skipped, uploaded_by, uploaded_at
        FROM dataset_uploads
        ORDER BY uploaded_at DESC, id DESC
        LIMIT 5
        """
    ).fetchall()
    conn.close()

    last_dataset_mount = None
    if last_dataset_upload:
        last_dataset_mount = {
            "file_name": last_dataset_upload["file_name"],
            "target_table": last_dataset_upload["target_table"],
            "records_imported": last_dataset_upload["records_imported"],
            "records_skipped": last_dataset_upload["records_skipped"],
            "uploaded_by": last_dataset_upload["uploaded_by"],
            "uploaded_at": format_db_datetime(last_dataset_upload["uploaded_at"]),
        }

    dataset_upload_history = []
    for row in dataset_upload_history_rows:
        dataset_upload_history.append(
            {
                "file_name": row["file_name"],
                "target_table": row["target_table"],
                "records_imported": row["records_imported"],
                "records_skipped": row["records_skipped"],
                "uploaded_by": row["uploaded_by"],
                "uploaded_at": format_db_datetime(row["uploaded_at"]),
            }
        )

    data_stats = {
        "total_consultas": total_consultas or 0,
        "fraudes_altos": fraudes_altos or 0,
        "total_logs": total_logs or 0,
        "last_data_event": format_db_datetime(last_data_event["created_at"]) if last_data_event else "Sin eventos",
        "last_data_message": last_data_event["message"] if last_data_event else "Aún no hay operaciones de datos.",
    }

    return render_page(
        "data_management",
        data_stats=data_stats,
        model_choices=MODEL_LABELS,
        last_dataset_mount=last_dataset_mount,
        dataset_upload_history=dataset_upload_history,
        notice=notice,
        error=error,
    )


@app.route("/gestion-datos/export/consultas.csv")
@login_required
@roles_required("administrador")
def export_data_consultas_csv():
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT c.id, c.created_at, u.username, c.monto, c.tipo_comercio, c.metodo_pago,
               c.ubicacion, c.frecuencia, c.hora, c.score, c.probabilidad_fraude,
               c.nivel_riesgo, c.recomendacion, c.modelo, c.resultado
        FROM consultas c
        LEFT JOIN users u ON u.id = c.user_id
        ORDER BY c.created_at DESC
        """
    ).fetchall()
    conn.close()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "id",
            "fecha",
            "usuario",
            "monto_usd",
            "tipo_comercio",
            "metodo_pago",
            "ubicacion",
            "frecuencia",
            "hora",
            "score",
            "probabilidad_fraude",
            "nivel_riesgo",
            "recomendacion",
            "modelo",
            "resultado",
        ]
    )

    for row in rows:
        writer.writerow(
            [
                row["id"],
                row["created_at"],
                row["username"],
                format_currency_usd(row["monto"]),
                row["tipo_comercio"],
                row["metodo_pago"],
                row["ubicacion"],
                row["frecuencia"],
                row["hora"],
                row["score"],
                row["probabilidad_fraude"],
                row["nivel_riesgo"],
                row["recomendacion"],
                row["modelo"],
                row["resultado"],
            ]
        )

    response = make_response(output.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = (
        f"attachment; filename=simdf_datos_consultas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    return response


@app.route("/gestion-datos/export/logs.csv")
@login_required
@roles_required("administrador")
def export_data_logs_csv():
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT id, created_at, event_type, severity, actor_username, message
        FROM system_logs
        ORDER BY created_at DESC
        """
    ).fetchall()
    conn.close()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "fecha", "tipo", "nivel", "actor", "mensaje"])

    for row in rows:
        writer.writerow(
            [
                row["id"],
                row["created_at"],
                row["event_type"],
                row["severity"],
                row["actor_username"],
                row["message"],
            ]
        )

    response = make_response(output.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = (
        f"attachment; filename=simdf_datos_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    return response


with app.app_context():
    init_db()


if __name__ == "__main__":
    app.run(debug=True)
