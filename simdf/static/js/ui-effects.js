(function () {
    const body = document.body;
    if (!body) {
        return;
    }

    const currentLanguage = (body.dataset.language || document.documentElement.lang || "es").toLowerCase();
    const isEnglish = currentLanguage.startsWith("en");

    const phraseEntries = [
            ["Sistema Interno de Monitoreo y Detección de Fraude", "Internal Fraud Monitoring and Detection System"],
            ["Evaluación profesional de riesgo transaccional", "Professional transaction risk assessment"],
            ["Modelo de evaluación fijo", "Fixed evaluation model"],
            ["Modelo utilizado", "Model used"],
            ["Iniciar sesión", "Sign in"],
            ["Ingresa tus credenciales corporativas", "Enter your corporate credentials"],
            ["ACCESO SEGURO", "SECURE ACCESS"],
            ["Mi perfil", "My profile"],
            ["Cerrar sesión", "Sign out"],
            ["Nueva transacción", "New transaction"],
            ["Métricas del modelo", "Model metrics"],
            ["Explicabilidad", "Explainability"],
            ["Acerca de", "About"],
            ["Gestión de usuarios", "User management"],
            ["Gestión de modelo", "Model management"],
            ["Gestión de datos", "Data management"],
            ["Seguridad del sistema", "System security"],
            ["Logs del sistema", "System logs"],
            ["Resultado Inteligente", "Smart result"],
            ["RESULTADO DEL ANALISIS", "ANALYSIS RESULT"],
            ["Probabilidad de fraude", "Fraud probability"],
            ["Recomendación automática", "Automatic recommendation"],
            ["Score de riesgo", "Risk score"],
            ["Dashboard General", "General dashboard"],
            ["NUEVA TRANSACCIÓN", "NEW TRANSACTION"],
            ["Simulador de Riesgo", "Risk simulator"],
            ["Monto de la transacción (USD)", "Transaction amount (USD)"],
            ["Monto de la transacción", "Transaction amount"],
            ["Cantidad de transacciones simuladas", "Number of simulated transactions"],
            ["Generar transacción simulada", "Generate simulated transaction"],
            ["Tipo generado", "Generated type"],
            ["Simuladas", "Simulated"],
            ["Fraudes detectados", "Detected frauds"],
            ["Tasa de fraude", "Fraud rate"],
            ["Método de pago", "Payment method"],
            ["Hora de la transacción (0-24)", "Transaction hour (0-24)"],
            ["Hora real de la transacción", "Real transaction time"],
            ["Frecuencia de transacciones recientes", "Recent transaction frequency"],
            ["Campos exactos del modelo entrenado (19 variables)", "Exact fields of the trained model (19 variables)"],
            ["Selecciona género", "Select gender"],
            ["Selecciona tipo de cuenta", "Select account type"],
            ["Selecciona tipo de transacción", "Select transaction type"],
            ["Selecciona categoría", "Select category"],
            ["Selecciona tipo de dispositivo", "Select device type"],
            ["Selecciona valor", "Select value"],
            ["Selecciona día", "Select day"],
            ["Selecciona mes", "Select month"],
            ["Hora (0-23)", "Hour (0-23)"],
            ["Tipo_Cuenta", "Account_Type"],
            ["Tipo_Transaccion", "Transaction_Type"],
            ["Categoria_Comercio", "Merchant_Category"],
            ["Balance_Cuenta_USD", "Account_Balance_USD"],
            ["Dispositivo_Transaccion", "Transaction_Device"],
            ["Tipo_Dispositivo", "Device_Type"],
            ["Porcentaje_Gasto", "Spending_Percentage"],
            ["Transaccion_Grande", "Large_Transaction"],
            ["Saldo_Restante", "Remaining_Balance"],
            ["Compra_Riesgosa", "Risky_Purchase"],
            ["Riesgo_Edad_Monto", "Age_Amount_Risk"],
            ["Dia_Semana", "Week_Day"],
            ["Mes", "Month"],
            ["Transaccion_Nocturna", "Night_Transaction"],
            ["Analizar Riesgo", "Analyze risk"],
            ["VALIDACIÓN CIENTÍFICA", "SCIENTIFIC VALIDATION"],
            ["Dashboard de Métricas del Modelo", "Model metrics dashboard"],
            ["Modelo listo para evaluación.", "Model ready for evaluation."],
            ["hasta integrar el dataset productivo.", "until the production dataset is integrated."],
            ["Historial de Consultas", "Query history"],
            ["EXPORTAR DATOS", "EXPORT DATA"],
            ["Subir dataset", "Upload dataset"],
            ["SUBIR DATASETS", "UPLOAD DATASETS"],
            ["Carga masiva desde CSV", "Bulk upload from CSV"],
            ["Archivo CSV", "CSV file"],
            ["LIMPIAR REGISTROS", "CLEAN RECORDS"],
            ["Depuración de datos históricos", "Historical data cleanup"],
            ["Limpiar registros", "Clean records"],
            ["Descargas para auditoría y respaldo", "Downloads for audit and backup"],
            ["Registros de consultas", "Query records"],
            ["Fraudes altos", "High fraud cases"],
            ["Eventos de log", "Log events"],
            ["Última operación", "Last operation"],
            ["Tema de la interfaz", "Interface theme"],
            ["Guardar configuración", "Save settings"],
            ["Todo el historial", "All history"],
            ["Solo consultas", "Queries only"],
            ["Solo logs", "Logs only"],
            ["Consultas y logs", "Queries and logs"],
            ["Centro de ayuda SIMDF", "SIMDF Help Center"],
            ["Escribe tu duda...", "Type your question..."],
            ["Procesando información...", "Processing information..."],
            ["Cargando SIMDF...", "Loading SIMDF..."],
            ["BLOQUEAR IP SOSPECHOSA", "BLOCK SUSPICIOUS IP"],
            ["Contención manual de acceso", "Manual access containment"],
            ["Dirección IP", "IP address"],
            ["Duración del bloqueo (minutos)", "Block duration (minutes)"],
            ["Motivo", "Reason"],
            ["Bloquear IP", "Block IP"],
            ["IPS BLOQUEADAS", "BLOCKED IPS"],
            ["Control de bloqueo y desbloqueo", "Block and unblock control"],
            ["VER INTENTOS DE LOGIN", "VIEW LOGIN ATTEMPTS"],
            ["Registro de autenticaciones", "Authentication records"],
            ["MONITOREAR ACCESOS", "MONITOR ACCESS"],
            ["Eventos de seguridad y autenticación", "Security and authentication events"],
            ["Intentos login 24h", "Login attempts 24h"],
            ["Fallidos 24h", "Failed 24h"],
            ["IPs bloqueadas activas", "Active blocked IPs"],
            ["IPs únicas 24h", "Unique IPs 24h"],
            ["Debes indicar una IP para bloquear.", "You must provide an IP address to block."],
            ["Debes indicar una IP para desbloquear.", "You must provide an IP address to unblock."],
            ["Credenciales inválidas", "Invalid credentials"],
            ["IP bloqueada temporalmente", "IP temporarily blocked"],
            ["Demasiados intentos fallidos.", "Too many failed attempts."],
            ["Desbloquear", "Unblock"],
            ["Exitoso", "Successful"],
            ["Fallido", "Failed"],
            ["Activa", "Active"],
            ["Expirada", "Expired"],
            ["Detalle", "Detail"],
            ["Fecha", "Date"],
            ["Estado", "Status"],
            ["Hasta", "Until"],
            ["Acción", "Action"],
            ["Por", "By"],
            ["Resultado", "Result"],
            ["Comercio", "Merchant"],
            ["Método", "Method"],
            ["Ubicación", "Location"],
            ["Ubicacion", "Location"],
            ["Recomendación", "Recommendation"],
            ["MÓDULO ACTIVO", "ACTIVE MODULE"],
            ["PERFIL", "PROFILE"],
            ["INFORMACIÓN", "INFORMATION"],
            ["ADMIN", "ADMIN"],
            ["Enviar", "Send"],
            ["Ayuda", "Help"],
            ["Ej:", "Ex:"]
    ];

    const tokenEntries = [
            ["Usuario", "User"],
            ["Contraseña", "Password"],
            ["Historial", "History"],
            ["Configuración", "Settings"],
            ["Idioma", "Language"],
            ["Riesgo", "Risk"],
            ["Monto", "Amount"],
            ["Frecuencia", "Frequency"],
            ["Hora", "Hour"],
            ["Resultado", "Result"],
            ["Mensaje", "Message"],
            ["Tipo", "Type"],
            ["Nivel", "Level"],
            ["Activo", "Active"],
            ["Bloqueado", "Blocked"],
            ["Consultas", "Queries"],
            ["Guardar", "Save"]
    ];

    const sortedPhraseEntries = phraseEntries.sort((a, b) => b[0].length - a[0].length);
    const sortedTokenEntries = tokenEntries.sort((a, b) => b[0].length - a[0].length);

    const translateText = (value) => {
        if (!isEnglish || !value || typeof value !== "string") {
            return value;
        }

        let result = value;
        sortedPhraseEntries.forEach(([source, target]) => {
            if (result.includes(source)) {
                result = result.split(source).join(target);
            }
        });

        sortedTokenEntries.forEach(([source, target]) => {
            if (result.includes(source)) {
                result = result.split(source).join(target);
            }
        });

        return result;
    };

    const translateElement = (root) => {
        if (!isEnglish || !root) {
            return;
        }

        const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
        const textNodes = [];
        while (walker.nextNode()) {
            textNodes.push(walker.currentNode);
        }

        textNodes.forEach((node) => {
            if (!node.parentElement) {
                return;
            }

            const tag = node.parentElement.tagName;
            if (tag === "SCRIPT" || tag === "STYLE") {
                return;
            }

            const translated = translateText(node.nodeValue);
            if (translated !== node.nodeValue) {
                node.nodeValue = translated;
            }
        });

        const targets = root.querySelectorAll
            ? root.querySelectorAll("*")
            : [];

        targets.forEach((el) => {
            ["placeholder", "title", "aria-label", "data-label", "data-message", "data-loading-text"].forEach((attr) => {
                if (el.hasAttribute(attr)) {
                    const translated = translateText(el.getAttribute(attr) || "");
                    el.setAttribute(attr, translated);
                }
            });

            if (el.tagName === "OPTION") {
                el.textContent = translateText(el.textContent || "");
            }
        });
    };

    const applyEnglishTranslations = () => {
        if (!isEnglish) {
            return;
        }
        translateElement(document.body);
    };

    applyEnglishTranslations();

    if (isEnglish && typeof MutationObserver !== "undefined") {
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                mutation.addedNodes.forEach((node) => {
                    if (node.nodeType === Node.TEXT_NODE && node.parentElement) {
                        const translated = translateText(node.nodeValue);
                        if (translated !== node.nodeValue) {
                            node.nodeValue = translated;
                        }
                        return;
                    }

                    if (node.nodeType === Node.ELEMENT_NODE) {
                        translateElement(node);
                    }
                });
            });
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    }

    const bindPasswordToggles = () => {
        const toggles = document.querySelectorAll(".password-toggle");
        toggles.forEach((toggle) => {
            if (toggle.dataset.bound === "true") {
                return;
            }

            toggle.dataset.bound = "true";
            toggle.addEventListener("click", () => {
                const fieldWrap = toggle.closest(".auth-input-wrap-password");
                let input = fieldWrap ? fieldWrap.querySelector("input") : null;

                if (!input) {
                    const targetId = toggle.getAttribute("data-password-toggle");
                    input = targetId ? document.getElementById(targetId) : null;
                }

                if (!input) {
                    return;
                }

                const show = input.type === "password";
                input.type = show ? "text" : "password";
                toggle.classList.toggle("is-active", show);
                toggle.setAttribute("aria-pressed", show ? "true" : "false");

                const icon = toggle.querySelector("i");
                if (icon) {
                    icon.className = show ? "bi bi-eye-slash" : "bi bi-eye";
                }

                const label = show
                    ? (isEnglish ? "Hide password" : "Ocultar contraseña")
                    : (isEnglish ? "Show password" : "Mostrar contraseña");
                toggle.setAttribute("aria-label", label);
                toggle.setAttribute("title", label);
            });
        });
    };

    bindPasswordToggles();

    body.classList.add("page-is-loading");

    const buildLoader = () => {
        if (document.getElementById("appLoader")) {
            return document.getElementById("appLoader");
        }

        const loader = document.createElement("div");
        loader.id = "appLoader";
        loader.className = "app-loader loader-theme-default";
        loader.innerHTML = `
            <div class="app-loader__content">
                <div class="app-loader__module" aria-hidden="true"><i class="bi bi-cpu app-loader__icon"></i></div>
                <div class="app-loader__ring" aria-hidden="true"></div>
                <p class="app-loader__text">${isEnglish ? "Loading SIMDF..." : "Cargando SIMDF..."}</p>
                <p class="app-loader__meta">${isEnglish ? "Preparing secure intelligence modules" : "Preparando modulos de inteligencia segura"}</p>
                <div class="app-loader__progress" aria-hidden="true"><span></span></div>
            </div>
        `;
        document.body.appendChild(loader);

        window.requestAnimationFrame(() => {
            loader.classList.add("is-visible");
        });

        return loader;
    };

    let loader = buildLoader();

    const setLoaderText = (text) => {
        const label = loader.querySelector(".app-loader__text");
        if (!label) {
            return;
        }
        label.textContent = text;
    };

    const setLoaderMeta = (text) => {
        const meta = loader.querySelector(".app-loader__meta");
        if (!meta) {
            return;
        }
        meta.textContent = text;
    };

    const setLoaderTheme = (themeName, iconClass) => {
        if (!loader) {
            return;
        }

        const themeClasses = [
            "loader-theme-default",
            "loader-theme-metrics",
            "loader-theme-transactions",
            "loader-theme-history",
            "loader-theme-security",
            "loader-theme-explainability",
            "loader-theme-data"
        ];

        themeClasses.forEach((themeClass) => loader.classList.remove(themeClass));
        loader.classList.add(themeName || "loader-theme-default");

        const iconNode = loader.querySelector(".app-loader__icon");
        if (iconNode) {
            iconNode.className = `${iconClass || "bi bi-cpu"} app-loader__icon`;
        }
    };

    const resolveLoaderContext = () => {
        const path = (window.location.pathname || "").toLowerCase();

        if (path.includes("metricas") || path.includes("model")) {
            return {
                meta: isEnglish ? "Calculating performance indicators" : "Calculando indicadores de desempeno",
                theme: "loader-theme-metrics",
                icon: "bi bi-graph-up-arrow"
            };
        }
        if (path.includes("nueva") || path.includes("predict") || path.includes("transaccion")) {
            return {
                meta: isEnglish ? "Validating transaction signals" : "Validando senales transaccionales",
                theme: "loader-theme-transactions",
                icon: "bi bi-cash-stack"
            };
        }
        if (path.includes("history") || path.includes("historial")) {
            return {
                meta: isEnglish ? "Loading traceability records" : "Cargando registros de trazabilidad",
                theme: "loader-theme-history",
                icon: "bi bi-clock-history"
            };
        }
        if (path.includes("seguridad") || path.includes("security") || path.includes("login")) {
            return {
                meta: isEnglish ? "Applying secure access controls" : "Aplicando controles de acceso seguro",
                theme: "loader-theme-security",
                icon: "bi bi-shield-lock"
            };
        }
        if (path.includes("explicabilidad") || path.includes("explainability")) {
            return {
                meta: isEnglish ? "Preparing model explainability factors" : "Preparando factores de explicabilidad del modelo",
                theme: "loader-theme-explainability",
                icon: "bi bi-lightbulb"
            };
        }
        if (path.includes("datos") || path.includes("data")) {
            return {
                meta: isEnglish ? "Synchronizing datasets and records" : "Sincronizando datasets y registros",
                theme: "loader-theme-data",
                icon: "bi bi-database-gear"
            };
        }

        return {
            meta: isEnglish ? "Preparing secure intelligence modules" : "Preparando modulos de inteligencia segura",
            theme: "loader-theme-default",
            icon: "bi bi-cpu"
        };
    };

    const showLoader = (text) => {
        if (!loader || !loader.isConnected) {
            loader = buildLoader();
        }

        const context = resolveLoaderContext();
        setLoaderText(text || (isEnglish ? "Processing information..." : "Procesando información..."));
        setLoaderMeta(context.meta);
        setLoaderTheme(context.theme, context.icon);
        loader.classList.remove("is-hidden");
        loader.classList.add("is-visible");
        body.classList.add("page-is-loading");
    };

    const hideLoader = () => {
        loader.classList.remove("is-visible");
        loader.classList.add("is-hidden");
        body.classList.remove("page-is-loading");

        window.setTimeout(() => {
            loader.remove();
        }, 650);
    };

    if (document.readyState === "complete") {
        hideLoader();
    } else {
        window.addEventListener("load", hideLoader, { once: true });
        window.setTimeout(hideLoader, 4200);
    }

    const submitForms = document.querySelectorAll("form");
    submitForms.forEach((form) => {
        form.addEventListener("submit", () => {
            if (form.dataset.submitting === "true") {
                return;
            }

            form.dataset.submitting = "true";
            const submitButton = form.querySelector("button[type='submit'], input[type='submit']");
            if (submitButton) {
                submitButton.disabled = true;
            }

            const fallbackText = isEnglish ? "Saving and processing data..." : "Guardando y procesando datos...";
            showLoader(form.dataset.loadingText || fallbackText);
        });
    });

    const authLoginForms = document.querySelectorAll("form[data-auth-login='true']");
    authLoginForms.forEach((form) => {
        if (form.dataset.boundLoginAnim === "true") {
            return;
        }

        form.dataset.boundLoginAnim = "true";
        form.addEventListener("submit", () => {
            const card = form.closest(".login-glass-card");
            if (card) {
                card.classList.add("auth-login-submitting");
            }
        });
    });

    const internalLinks = document.querySelectorAll("a[href]");
    internalLinks.forEach((link) => {
        const href = link.getAttribute("href") || "";
        if (!href || href.startsWith("#") || href.startsWith("javascript:")) {
            return;
        }

        if (link.target && link.target !== "_self") {
            return;
        }

        if (link.hasAttribute("download")) {
            return;
        }

        link.addEventListener("click", (event) => {
            if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
                return;
            }

            if (href.startsWith("mailto:") || href.startsWith("tel:")) {
                return;
            }

            const current = `${window.location.pathname}${window.location.search}`;
            if (href === current) {
                return;
            }

            showLoader(isEnglish ? "Opening module..." : "Abriendo módulo...");
            document.body.classList.add("page-transition-out");
        });
    });

    const toasts = document.querySelectorAll("[data-toast]");
    toasts.forEach((toast) => {
        const closeButton = toast.querySelector("[data-toast-close]");
        const duration = Number(toast.dataset.toastDuration || 4200);

        const closeToast = () => {
            toast.classList.remove("is-visible");
            toast.classList.add("is-hiding");
            window.setTimeout(() => {
                toast.remove();
            }, 260);
        };

        if (closeButton) {
            closeButton.addEventListener("click", closeToast);
        }

        window.requestAnimationFrame(() => {
            toast.classList.add("is-visible");
        });

        if (duration > 0) {
            window.setTimeout(closeToast, duration);
        }
    });

    const notifyNodes = document.querySelectorAll(".js-notify");
    if (notifyNodes.length && window.Swal) {
        const labels = isEnglish
            ? {
                success: "Operation completed",
                error: "Error",
                info: "Information"
            }
            : {
                success: "Operacion exitosa",
                error: "Error",
                info: "Informacion"
            };

        notifyNodes.forEach((item, index) => {
            const type = item.dataset.type || "info";
            const message = translateText(item.dataset.message || "");
            const title = labels[type] || labels.info;

            window.setTimeout(() => {
                Swal.fire({
                    toast: true,
                    position: "top-end",
                    icon: type,
                    title,
                    text: message,
                    showConfirmButton: false,
                    timer: 3600,
                    timerProgressBar: true,
                    customClass: {
                        popup: "simdf-swal-toast"
                    }
                });
            }, index * 220);
        });

        document.querySelectorAll(".status").forEach((node) => {
            node.style.display = "none";
        });
    }

    const progressBars = document.querySelectorAll(".risk-meter, .explain-track, .trend-col");
    if (progressBars.length && !window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
        const animateProgress = (element, duration) => {
            const target = Number(element.value || 0);
            if (!Number.isFinite(target) || target <= 0) {
                return;
            }

            const startValue = 0;
            const startTime = performance.now();
            element.value = startValue;

            const step = (now) => {
                const elapsed = now - startTime;
                const t = Math.min(1, elapsed / duration);
                const eased = 1 - Math.pow(1 - t, 3);
                element.value = Math.round(startValue + (target - startValue) * eased);

                if (t < 1) {
                    requestAnimationFrame(step);
                } else {
                    element.value = target;
                }
            };

            requestAnimationFrame(step);
        };

        progressBars.forEach((bar, index) => {
            window.setTimeout(() => {
                animateProgress(bar, 760 + Math.min(index * 75, 420));
            }, 140 + index * 40);
        });
    }

    const trendCanvas = document.getElementById("riskTrendChart");
    if (trendCanvas && window.Chart) {
        const raw = trendCanvas.dataset.probabilities || "[]";
        let probabilities = [];

        try {
            probabilities = JSON.parse(raw);
        } catch (error) {
            probabilities = [];
        }

        if (Array.isArray(probabilities) && probabilities.length) {
            const labels = probabilities.map((_, index) => `Tx ${index + 1}`);
            const pointColors = probabilities.map((value) => {
                if (value >= 70) {
                    return "#de5a4a";
                }
                if (value >= 40) {
                    return "#e0ad38";
                }
                return "#2fb97f";
            });

            const lineGradient = trendCanvas
                .getContext("2d")
                .createLinearGradient(0, 0, 0, 240);
            lineGradient.addColorStop(0, "rgba(39, 166, 179, 0.32)");
            lineGradient.addColorStop(1, "rgba(39, 166, 179, 0.04)");

            new Chart(trendCanvas, {
                type: "line",
                data: {
                    labels,
                    datasets: [
                        {
                            label: isEnglish ? "Fraud probability" : "Probabilidad de fraude",
                            data: probabilities,
                            borderColor: "#1f6e86",
                            borderWidth: 2,
                            fill: true,
                            backgroundColor: lineGradient,
                            tension: 0.36,
                            pointRadius: 5,
                            pointHoverRadius: 6,
                            pointBorderWidth: 2,
                            pointBackgroundColor: pointColors,
                            pointBorderColor: "#ffffff"
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => `${isEnglish ? "Risk" : "Riesgo"}: ${context.raw}%`
                            }
                        }
                    },
                    scales: {
                        x: {
                            grid: {
                                color: "rgba(118, 153, 184, 0.14)"
                            },
                            ticks: {
                                color: "#486783"
                            }
                        },
                        y: {
                            min: 0,
                            max: 100,
                            grid: {
                                color: "rgba(118, 153, 184, 0.16)"
                            },
                            ticks: {
                                color: "#486783",
                                callback: (value) => `${value}%`
                            }
                        }
                    }
                }
            });
        }
    }

    const riskDistributionCanvas = document.getElementById("riskDistributionChart");
    if (riskDistributionCanvas && window.Chart) {
        let distribution = { alto: 0, medio: 0, bajo: 0 };
        try {
            distribution = JSON.parse(riskDistributionCanvas.dataset.riskDistribution || "{}");
        } catch (error) {
            distribution = { alto: 0, medio: 0, bajo: 0 };
        }

        const donutData = [
            Number(distribution.alto || 0),
            Number(distribution.medio || 0),
            Number(distribution.bajo || 0)
        ];

        if (donutData.some((value) => value > 0)) {
            new Chart(riskDistributionCanvas, {
                type: "doughnut",
                data: {
                    labels: ["Riesgo alto", "Riesgo medio", "Riesgo bajo"],
                    datasets: [
                        {
                            data: donutData,
                            backgroundColor: ["#de5a4a", "#e0ad38", "#2fb97f"],
                            borderColor: "#ffffff",
                            borderWidth: 2,
                            hoverOffset: 8
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: "bottom",
                            labels: {
                                color: "#486783",
                                boxWidth: 12,
                                boxHeight: 12
                            }
                        }
                    },
                    cutout: "62%"
                }
            });
        }
    }

    const fraudTimeCanvas = document.getElementById("fraudTimeChart");
    if (fraudTimeCanvas && window.Chart) {
        let trendSeries = { day: { labels: [], values: [] }, week: { labels: [], values: [] }, month: { labels: [], values: [] } };
        try {
            trendSeries = JSON.parse(fraudTimeCanvas.dataset.fraudTrend || "{}");
        } catch (error) {
            trendSeries = { day: { labels: [], values: [] }, week: { labels: [], values: [] }, month: { labels: [], values: [] } };
        }

        const getSeries = (key) => {
            const source = trendSeries[key] || { labels: [], values: [] };
            return {
                labels: Array.isArray(source.labels) ? source.labels : [],
                values: Array.isArray(source.values) ? source.values : []
            };
        };

        const daySeries = getSeries("day");
        const trendChart = new Chart(fraudTimeCanvas, {
            type: "line",
            data: {
                labels: daySeries.labels,
                datasets: [
                    {
                        label: isEnglish ? "Fraud alerts" : "Alertas de fraude",
                        data: daySeries.values,
                        borderColor: "#1f6e86",
                        backgroundColor: "rgba(39, 166, 179, 0.18)",
                        borderWidth: 2,
                        fill: true,
                        tension: 0.35,
                        pointRadius: 4,
                        pointHoverRadius: 5,
                        pointBackgroundColor: "#2b88a2",
                        pointBorderColor: "#ffffff",
                        pointBorderWidth: 2
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        grid: {
                            color: "rgba(118, 153, 184, 0.14)"
                        },
                        ticks: {
                            color: "#486783"
                        }
                    },
                    y: {
                        beginAtZero: true,
                        precision: 0,
                        grid: {
                            color: "rgba(118, 153, 184, 0.16)"
                        },
                        ticks: {
                            color: "#486783"
                        }
                    }
                }
            }
        });

        const rangeButtons = document.querySelectorAll(".fraud-time-btn[data-trend-range]");
        rangeButtons.forEach((button) => {
            button.addEventListener("click", () => {
                const range = button.getAttribute("data-trend-range") || "day";
                const series = getSeries(range);

                trendChart.data.labels = series.labels;
                trendChart.data.datasets[0].data = series.values;
                trendChart.update();

                rangeButtons.forEach((item) => item.classList.remove("is-active"));
                button.classList.add("is-active");
            });
        });
    }

    const fraudRealMap = document.getElementById("fraudRealMap");
    if (fraudRealMap && window.L) {
        let mapPoints = [];
        try {
            mapPoints = JSON.parse(fraudRealMap.dataset.mapPoints || "[]");
        } catch (error) {
            mapPoints = [];
        }

        if (Array.isArray(mapPoints) && mapPoints.length > 0) {
            const baseCenter = [4.5709, -74.2973];
            const map = L.map(fraudRealMap, {
                zoomControl: true,
                scrollWheelZoom: false
            }).setView(baseCenter, 5);

            L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
                maxZoom: 18,
                attribution: "&copy; OpenStreetMap contributors"
            }).addTo(map);

            const bounds = [];
            mapPoints.forEach((point) => {
                const lat = Number(point.lat);
                const lng = Number(point.lng);
                const alerts = Number(point.alerts || 0);

                if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
                    return;
                }

                const radius = Math.max(8, Math.min(24, 8 + alerts * 1.6));
                const marker = L.circleMarker([lat, lng], {
                    radius,
                    color: "#a6362f",
                    weight: 2,
                    fillColor: "#e4604f",
                    fillOpacity: 0.65
                }).addTo(map);

                marker.bindPopup(`<strong>${point.city}</strong><br>${alerts} eventos de riesgo`);
                bounds.push([lat, lng]);
            });

            if (bounds.length === 1) {
                map.setView(bounds[0], 7);
            } else if (bounds.length > 1) {
                map.fitBounds(bounds, {
                    padding: [24, 24]
                });
            }

            window.setTimeout(() => {
                map.invalidateSize();
            }, 180);
        }
    }

    const geolocationForms = document.querySelectorAll("form[data-geolocation-form='true']");
    geolocationForms.forEach((form) => {
        const trigger = form.querySelector("[data-detect-location]");
        const locationSelect = form.querySelector("select[name='ubicacion']");
        const locationDisplay = form.querySelector("#geo_location_display");
        const locationLabelInput = form.querySelector("#geo_location_label");
        const latitudeInput = form.querySelector("#geo_latitude");
        const longitudeInput = form.querySelector("#geo_longitude");
        const realHourInput = form.querySelector("#real_transaction_hour");
        const horaDisplayInput = form.querySelector("#hora");
        const status = form.querySelector("#geo_status_message");

        if (!trigger) {
            return;
        }

        const setStatus = (text, isError) => {
            if (!status) {
                return;
            }
            status.textContent = text;
            status.classList.toggle("is-error", Boolean(isError));
            status.classList.toggle("is-success", !isError);
        };

        const inferRiskLocation = (cityName, countryCode) => {
            const city = (cityName || "").toLowerCase();
            const country = (countryCode || "").toLowerCase();

            if (country !== "co") {
                return "internacional";
            }

            if (city.includes("bogota") || city.includes("bogotá")) {
                return "local";
            }

            return "nacional";
        };

        const reverseLookup = async (lat, lng) => {
            const endpoint = `https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lng)}&accept-language=es`;
            const response = await fetch(endpoint, {
                headers: {
                    "Accept": "application/json"
                }
            });

            if (!response.ok) {
                throw new Error("reverse_lookup_failed");
            }

            return response.json();
        };

        const detectLocation = () => {
            if (!navigator.geolocation) {
                setStatus(isEnglish ? "Geolocation is not available in this browser." : "La geolocalización no está disponible en este navegador.", true);
                return Promise.resolve(false);
            }

            trigger.disabled = true;
            setStatus(isEnglish ? "Detecting your location..." : "Detectando tu ubicación...", false);

            return new Promise((resolve) => {
                navigator.geolocation.getCurrentPosition(
                    async (position) => {
                        const lat = Number(position.coords.latitude.toFixed(6));
                        const lng = Number(position.coords.longitude.toFixed(6));
                        let cityName = "";
                        let countryName = "";
                        let countryCode = "";
                        let localityName = "";

                        try {
                            const reverse = await reverseLookup(lat, lng);
                            const addr = reverse.address || {};
                            cityName = addr.city || addr.town || addr.village || addr.municipality || addr.state || "";
                            localityName = addr.suburb || addr.city_district || addr.neighbourhood || "";
                            countryName = addr.country || "";
                            countryCode = addr.country_code || "";
                        } catch (error) {
                            cityName = "";
                            localityName = "";
                            countryName = "";
                            countryCode = "";
                        }

                        let label = "";
                        if (localityName && cityName && countryName) {
                            label = `${localityName}, ${cityName}, ${countryName}`;
                        } else if (cityName && countryName) {
                            label = `${cityName}, ${countryName}`;
                        } else {
                            label = `${lat}, ${lng}`;
                        }

                        if (locationDisplay) {
                            locationDisplay.value = label;
                        }
                        if (locationLabelInput) {
                            locationLabelInput.value = label;
                        }
                        if (latitudeInput) {
                            latitudeInput.value = String(lat);
                        }
                        if (longitudeInput) {
                            longitudeInput.value = String(lng);
                        }

                        if (locationSelect) {
                            locationSelect.value = inferRiskLocation(cityName, countryCode);
                        }

                        setStatus(isEnglish ? "Real location detected and assigned to the transaction." : "Ubicación real detectada y asignada a la transacción.", false);
                        trigger.disabled = false;
                        resolve(true);
                    },
                    (error) => {
                        let message = isEnglish
                            ? "Could not detect location."
                            : "No se pudo detectar la ubicación.";

                        if (error && error.code === 1) {
                            message = isEnglish
                                ? "Permission denied. Enable location permissions to use this feature."
                                : "Permiso denegado. Activa los permisos de ubicación para usar esta función.";
                        } else if (error && error.code === 3) {
                            message = isEnglish
                                ? "Location detection timed out. Please try again."
                                : "La detección de ubicación tardó demasiado. Intenta de nuevo.";
                        }

                        setStatus(message, true);
                        trigger.disabled = false;
                        resolve(false);
                    },
                    {
                        enableHighAccuracy: true,
                        timeout: 12000,
                        maximumAge: 0
                    }
                );
            });
        };

        const syncRealTransactionTime = () => {
            const now = new Date();
            const decimalHour = now.getHours() + (now.getMinutes() / 60) + (now.getSeconds() / 3600);
            const displayHour = String(now.getHours()).padStart(2, "0");
            const displayMinute = String(now.getMinutes()).padStart(2, "0");

            if (realHourInput) {
                realHourInput.value = decimalHour.toFixed(2);
            }
            if (horaDisplayInput) {
                horaDisplayInput.value = `${displayHour}:${displayMinute}`;
            }
        };

        trigger.addEventListener("click", () => {
            detectLocation();
        });

        form.addEventListener("submit", (event) => {
            syncRealTransactionTime();

            const hasGeo = latitudeInput && longitudeInput && latitudeInput.value && longitudeInput.value;
            if (hasGeo) {
                return;
            }

            event.preventDefault();
            setStatus(isEnglish ? "You must detect your real location before sending the transaction." : "Debes detectar tu ubicación real antes de enviar la transacción.", true);
        });

        if (!locationLabelInput || !locationLabelInput.value) {
            detectLocation();
        }

        syncRealTransactionTime();
        window.setInterval(syncRealTransactionTime, 15000);
    });

    const dashboardForms = document.querySelectorAll("form.dashboard-form");
    dashboardForms.forEach((form) => {
        const resetButton = form.querySelector("button[type='reset']");
        if (!resetButton) {
            return;
        }

        resetButton.addEventListener("click", () => {
            form.reset();

            const simulateStats = form.querySelector("[data-simulate-stats]");
            if (simulateStats) {
                const simCountTotal = simulateStats.querySelector("[data-sim-count-total]");
                const simCountFraud = simulateStats.querySelector("[data-sim-count-fraud]");
                const simCountRate = simulateStats.querySelector("[data-sim-count-rate]");
                if (simCountTotal) simCountTotal.textContent = "0";
                if (simCountFraud) simCountFraud.textContent = "0";
                if (simCountRate) simCountRate.textContent = "0%";
            }

            const geoDisplay = form.querySelector("#geo_location_display");
            const geoLabel = form.querySelector("#geo_location_label");
            const geoLat = form.querySelector("#geo_latitude");
            const geoLng = form.querySelector("#geo_longitude");
            if (geoDisplay) geoDisplay.value = "";
            if (geoLabel) geoLabel.value = "";
            if (geoLat) geoLat.value = "";
            if (geoLng) geoLng.value = "";

            const simulateOutput = form.querySelector("[data-simulate-output]");
            if (simulateOutput) {
                simulateOutput.textContent = "";
                simulateOutput.hidden = true;
            }

            const geoStatus = form.querySelector("#geo_status_message");
            if (geoStatus) {
                geoStatus.textContent = isEnglish
                    ? "You must detect your real location to send the transaction with actual location."
                    : "Debes detectar tu ubicación actual para enviar la transacción con localización real.";
                geoStatus.classList.remove("is-error", "is-success");
            }
        });
    });

    const dashboardClearBtn = document.getElementById("dashboardClearBtn");
    if (dashboardClearBtn) {
        dashboardClearBtn.addEventListener("click", async () => {
            const confirmMsg = isEnglish
                ? "Clear all dashboard data? This will delete all transactions and start fresh."
                : "¿Limpiar todos los datos del dashboard? Se eliminarán todas las transacciones y comenzarás desde cero.";

            let confirmed = false;
            if (window.Swal) {
                const result = await Swal.fire({
                    icon: "warning",
                    title: isEnglish ? "Clear dashboard data" : "Limpiar datos del dashboard",
                    text: confirmMsg,
                    showCancelButton: true,
                    confirmButtonText: isEnglish ? "Yes, clear" : "Sí, limpiar",
                    cancelButtonText: isEnglish ? "Cancel" : "Cancelar",
                    reverseButtons: true,
                    customClass: {
                        popup: "simdf-swal-modal",
                        confirmButton: "simdf-swal-confirm",
                        cancelButton: "simdf-swal-cancel"
                    },
                    buttonsStyling: false
                });
                confirmed = Boolean(result.isConfirmed);
            } else {
                confirmed = confirm(confirmMsg);
            }

            if (!confirmed) {
                return;
            }

            try {
                const response = await fetch("/api/clear-dashboard", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                });

                if (response.ok) {
                    await response.json();
                    window.setTimeout(() => {
                        location.reload();
                    }, 500);
                } else {
                    const error = await response.json();
                    const message = isEnglish
                        ? "Error clearing dashboard: " + error.mensaje
                        : "Error limpiando dashboard: " + error.mensaje;

                    if (window.Swal) {
                        await Swal.fire({
                            icon: "error",
                            title: isEnglish ? "Could not clear" : "No se pudo limpiar",
                            text: message,
                            customClass: {
                                popup: "simdf-swal-modal",
                                confirmButton: "simdf-swal-confirm"
                            },
                            buttonsStyling: false
                        });
                    } else {
                        alert(message);
                    }
                }
            } catch (error) {
                console.error("Error:", error);
                const fallback = isEnglish ? "Error clearing dashboard" : "Error limpiando dashboard";
                if (window.Swal) {
                    await Swal.fire({
                        icon: "error",
                        title: isEnglish ? "Connection error" : "Error de conexión",
                        text: fallback,
                        customClass: {
                            popup: "simdf-swal-modal",
                            confirmButton: "simdf-swal-confirm"
                        },
                        buttonsStyling: false
                    });
                } else {
                    alert(fallback);
                }
            }
        });
    }

    const simulationButtons = document.querySelectorAll("[data-simulate-transaction]");

    const updateDashboard = (data) => {
        const transactions = Array.isArray(data) ? data : [];
        console.log("[SIMDF] data recibido:", transactions);

        const total = transactions.length;
        const fraudCount = transactions.filter((tx) => String((tx && tx.clasificacion) || "").toLowerCase() === "fraude").length;

        const riskScoreSum = transactions.reduce((acc, tx) => {
            const score = Number(tx && tx.risk_score);
            return acc + (Number.isFinite(score) ? score : 0);
        }, 0);

        const avgRiskScore = total > 0 ? (riskScoreSum / total) : 0;
        const avgRiskNorm = Math.max(0, Math.min(1, avgRiskScore / 3));
        const fraudePct = total > 0 ? (fraudCount / total) * 100 : 0;
        const riesgoPct = avgRiskNorm * 100;

        console.log("[SIMDF] promedio calculado (risk_score):", avgRiskScore, "normalizado:", avgRiskNorm);

        let riskLevel = "BAJO";
        let riskTitle = "Riesgo bajo";
        let riskTrafficText = "Bajo";
        let riskTrafficClass = "risk-traffic-bajo";
        let chipClass = "chip-bajo";
        let heroClass = "risk-hero-bajo";
        let meterClass = "risk-fill-bajo";
        let recommendation = "Recomendación: Mantener monitoreo";

        if (total === 0 || avgRiskNorm === 0) {
            riskLevel = "BAJO";
            riskTitle = "Sin riesgo calculado";
            riskTrafficText = "Bajo";
            recommendation = "Recomendación: Sin riesgo calculado";
        } else if (avgRiskNorm >= 0.7) {
            riskLevel = "ALTO";
            riskTitle = "Alto riesgo de fraude";
            riskTrafficText = "Alto";
            riskTrafficClass = "risk-traffic-alto";
            chipClass = "chip-alto";
            heroClass = "risk-hero-alto";
            meterClass = "risk-fill-alto";
            recommendation = "Recomendación: Bloquear y escalar alerta";
        } else if (avgRiskNorm >= 0.3) {
            riskLevel = "MEDIO";
            riskTitle = "Riesgo moderado";
            riskTrafficText = "Medio";
            riskTrafficClass = "risk-traffic-medio";
            chipClass = "chip-medio";
            heroClass = "risk-hero-medio";
            meterClass = "risk-fill-medio";
            recommendation = "Recomendación: Revisar manualmente";
        }

        const summaryGrid = document.getElementById("dashboard");
        if (summaryGrid) {
            const summaryValues = summaryGrid.querySelectorAll(".summary-card .summary-value");
            if (summaryValues[0]) {
                summaryValues[0].textContent = String(total);
            }
            if (summaryValues[1]) {
                summaryValues[1].textContent = fraudePct.toFixed(1) + "%";
            }
            if (summaryValues[2]) {
                summaryValues[2].textContent = riskLevel;
                summaryValues[2].classList.remove("chip-bajo", "chip-medio", "chip-alto");
                summaryValues[2].classList.add(chipClass);
            }
            if (summaryValues[3]) {
                summaryValues[3].textContent = String(fraudCount);
            }
        }

        const riskHero = document.querySelector(".risk-hero");
        if (riskHero) {
            const heroTitle = riskHero.querySelector(".risk-hero-title");
            const heroProb = riskHero.querySelector(".risk-hero-prob");
            const heroTraffic = riskHero.querySelector(".risk-hero-traffic");
            const riskMeterLabel = riskHero.querySelector(".risk-meter-label");
            const riskProgress = riskHero.querySelector(".risk-meter");
            const heroRec = riskHero.querySelector(".risk-hero-rec");

            riskHero.classList.remove("risk-hero-bajo", "risk-hero-medio", "risk-hero-alto");
            riskHero.classList.add(heroClass);

            if (heroTitle) {
                heroTitle.textContent = riskTitle;
            }
            if (heroProb) {
                heroProb.textContent = riesgoPct.toFixed(1) + "% de probabilidad de fraude";
            }
            if (heroTraffic) {
                heroTraffic.innerHTML = '<span class="risk-traffic ' + riskTrafficClass + '">● ' + riskTrafficText + "</span>";
            }
            if (riskMeterLabel) {
                riskMeterLabel.textContent = "Riesgo: " + riesgoPct.toFixed(1) + "%";
            }
            if (riskProgress) {
                riskProgress.value = Math.max(0, Math.min(100, riesgoPct));
                riskProgress.classList.remove("risk-fill-bajo", "risk-fill-medio", "risk-fill-alto");
                riskProgress.classList.add(meterClass);
            }
            if (heroRec) {
                heroRec.textContent = recommendation;
            }
        }

        const statBoxes = document.querySelectorAll("[data-simulate-stats]");
        statBoxes.forEach((statsBox) => {
            const totalCounter = statsBox.querySelector("[data-sim-count-total]");
            const fraudCounter = statsBox.querySelector("[data-sim-count-fraud]");
            const rateCounter = statsBox.querySelector("[data-sim-count-rate]");

            if (totalCounter) {
                totalCounter.textContent = String(total);
            }
            if (fraudCounter) {
                fraudCounter.textContent = String(fraudCount);
            }
            if (rateCounter) {
                rateCounter.textContent = fraudePct.toFixed(1) + "%";
            }
        });

        return {
            total,
            fraud: fraudCount,
            avgRiskScore,
            avgRiskNorm
        };
    };

    const syncDashboardWithApiTransactions = async () => {
        const summaryGrid = document.getElementById("dashboard");
        const hasSimulationStats = document.querySelector("[data-simulate-stats]");
        const riskHero = document.querySelector(".risk-hero");
        if (!summaryGrid && !hasSimulationStats && !riskHero) {
            return null;
        }

        try {
            const response = await fetch("/api/transactions", {
                method: "GET",
                credentials: "same-origin",
                headers: {
                    "Accept": "application/json"
                }
            });

            if (!response.ok) {
                throw new Error("api_transactions_http_" + String(response.status));
            }

            const payload = await response.json();
            const transactions = Array.isArray(payload && payload.transactions) ? payload.transactions : [];
            return updateDashboard(transactions);
        } catch (error) {
            console.log("[SIMDF] Error syncing /api/transactions:", error);
            return updateDashboard([]);
        }
    };

    const initialApiStatsPromise = syncDashboardWithApiTransactions();

    simulationButtons.forEach((button) => {
        const form = button.closest("form");
        if (!form) {
            return;
        }

        const output = form.querySelector("[data-simulate-output]");
        const statsBox = form.querySelector("[data-simulate-stats]");
        const totalCounter = form.querySelector("[data-sim-count-total]");
        const fraudCounter = form.querySelector("[data-sim-count-fraud]");
        const rateCounter = form.querySelector("[data-sim-count-rate]");
        const statsStorageKey = "simdfSimulationCounters";
        const fields = {
            modelMontoUsd: form.querySelector("input[name='model_monto_usd']"),
            simulationCount: form.querySelector("input[name='simulation_count']"),
            hora: form.querySelector("input[name='hora']"),
            realHour: form.querySelector("#real_transaction_hour"),
            modelGenero: form.querySelector("select[name='model_genero']"),
            modelEdad: form.querySelector("input[name='model_edad']"),
            modelCiudad: form.querySelector("input[name='model_ciudad']"),
            modelTipoCuenta: form.querySelector("select[name='model_tipo_cuenta']"),
            modelTipoTransaccion: form.querySelector("select[name='model_tipo_transaccion']"),
            modelCategoriaComercio: form.querySelector("select[name='model_categoria_comercio']"),
            modelBalanceCuenta: form.querySelector("input[name='model_balance_cuenta_usd']"),
            modelDispositivoTransaccion: form.querySelector("input[name='model_dispositivo_transaccion']"),
            modelTipoDispositivo: form.querySelector("select[name='model_tipo_dispositivo']"),
            modelPorcentajeGasto: form.querySelector("input[name='model_porcentaje_gasto']"),
            modelTransaccionGrande: form.querySelector("select[name='model_transaccion_grande']"),
            modelSaldoRestante: form.querySelector("input[name='model_saldo_restante']"),
            modelCompraRiesgosa: form.querySelector("select[name='model_compra_riesgosa']"),
            modelRiesgoEdadMonto: form.querySelector("input[name='model_riesgo_edad_monto']"),
            modelDiaSemana: form.querySelector("select[name='model_dia_semana']"),
            modelMes: form.querySelector("select[name='model_mes']"),
            modelHora: form.querySelector("input[name='model_hora']"),
            modelTransaccionNocturna: form.querySelector("select[name='model_transaccion_nocturna']"),
            geoDisplay: form.querySelector("#geo_location_display"),
            geoLabel: form.querySelector("#geo_location_label"),
            geoLat: form.querySelector("#geo_latitude"),
            geoLng: form.querySelector("#geo_longitude"),
            geoStatus: form.querySelector("#geo_status_message")
        };

        const sanitizeCounter = (value) => {
            const n = Number(value);
            if (!Number.isFinite(n) || n < 0) {
                return 0;
            }
            return Math.floor(n);
        };

        const getStoredStats = () => {
            try {
                const raw = window.sessionStorage.getItem(statsStorageKey);
                if (!raw) {
                    return { total: 0, fraud: 0 };
                }
                const parsed = JSON.parse(raw);
                const total = sanitizeCounter(parsed.total);
                const fraud = Math.min(sanitizeCounter(parsed.fraud), total);
                return { total, fraud };
            } catch (_error) {
                return { total: 0, fraud: 0 };
            }
        };

        const saveStats = (stats) => {
            try {
                window.sessionStorage.setItem(
                    statsStorageKey,
                    JSON.stringify({
                        total: sanitizeCounter(stats.total),
                        fraud: sanitizeCounter(stats.fraud)
                    })
                );
            } catch (_error) {
                // Ignore storage errors (private mode / quota), counters still work in-memory for this render.
            }
        };

        const renderStats = (stats) => {
            if (!statsBox) {
                return;
            }

            const total = sanitizeCounter(stats.total);
            const fraud = Math.min(sanitizeCounter(stats.fraud), total);
            const rate = total > 0 ? ((fraud / total) * 100).toFixed(1) : "0.0";

            if (totalCounter) {
                totalCounter.textContent = String(total);
            }
            if (fraudCounter) {
                fraudCounter.textContent = String(fraud);
            }
            if (rateCounter) {
                rateCounter.textContent = `${rate}%`;
            }
        };

        const getRequestedSimulationCount = () => {
            const raw = fields.simulationCount ? fields.simulationCount.value : "1";
            const parsed = Number.parseInt(raw, 10);
            if (!Number.isFinite(parsed)) {
                return 1;
            }
            return Math.max(1, Math.min(200, parsed));
        };

        let simulationStats = getStoredStats();
        renderStats(simulationStats);

        if (initialApiStatsPromise && typeof initialApiStatsPromise.then === "function") {
            initialApiStatsPromise.then((apiStats) => {
                if (!apiStats) {
                    return;
                }

                simulationStats = {
                    total: sanitizeCounter(apiStats.total),
                    fraud: sanitizeCounter(apiStats.fraud)
                };
                renderStats(simulationStats);
                saveStats(simulationStats);
            });
        }

        const setOutput = (text, isError) => {
            if (!output) {
                return;
            }
            output.hidden = false;
            output.textContent = text;
            output.classList.toggle("is-error", Boolean(isError));
        };

        const fillFields = (tx) => {
            if (!tx) {
                return;
            }

            if (fields.modelMontoUsd) {
                fields.modelMontoUsd.value = String(tx.monto ?? "");
            }
            if (fields.hora) {
                const hourDecimal = Number(tx.hora ?? 0);
                const safeHour = Number.isFinite(hourDecimal) ? Math.max(0, Math.min(23.99, hourDecimal)) : 0;
                const hourPart = Math.floor(safeHour);
                const minutePart = Math.round((safeHour - hourPart) * 60);
                const displayHour = String(hourPart).padStart(2, "0");
                const displayMinute = String(Math.min(59, minutePart)).padStart(2, "0");
                fields.hora.value = `${displayHour}:${displayMinute}`;
            }
            if (fields.realHour) {
                fields.realHour.value = String(tx.hora ?? "");
            }

            const montoValue = Number(tx.monto ?? 0);
            const hourDecimal = Number(tx.hora ?? 0);
            const safeHour = Number.isFinite(hourDecimal) ? Math.max(0, Math.min(23, Math.floor(hourDecimal))) : 0;
            const balance = Math.max(montoValue * 2.2, 450);
            const porcentajeGasto = balance > 0 ? ((montoValue / balance) * 100) : 0;
            const saldoRestante = Math.max(balance - montoValue, 0);
            const day = new Date();
            const diaSemana = day.getDay() === 0 ? 7 : day.getDay();
            const mes = day.getMonth() + 1;

            if (fields.modelGenero) {
                fields.modelGenero.value = "Masculino";
            }
            if (fields.modelEdad) {
                fields.modelEdad.value = "35";
            }
            if (fields.modelCiudad) {
                fields.modelCiudad.value = "Bangalore";
            }
            if (fields.modelTipoCuenta) {
                fields.modelTipoCuenta.value = "Ahorros";
            }
            if (fields.modelTipoTransaccion) {
                const txMap = {
                    debito: "Débito",
                    credito: "Crédito",
                    transferencia: "Transferencia",
                    billetera: "Pago de Factura"
                };
                fields.modelTipoTransaccion.value = txMap[String(tx.metodo_pago || "")] || "Transferencia";
            }
            if (fields.modelCategoriaComercio) {
                const categoryMap = {
                    retail: "Supermercado",
                    restaurante: "Restaurante",
                    gasolinera: "Entretenimiento",
                    ecommerce: "Electrónica",
                    transferencia: "Electrónica"
                };
                fields.modelCategoriaComercio.value = categoryMap[String(tx.tipo_comercio || "")] || "Supermercado";
            }
            if (fields.modelBalanceCuenta) {
                fields.modelBalanceCuenta.value = balance.toFixed(2);
            }
            if (fields.modelDispositivoTransaccion) {
                fields.modelDispositivoTransaccion.value = "POS-Terminal-12";
            }
            if (fields.modelTipoDispositivo) {
                fields.modelTipoDispositivo.value = "Computador";
            }
            if (fields.modelPorcentajeGasto) {
                fields.modelPorcentajeGasto.value = porcentajeGasto.toFixed(2);
            }
            if (fields.modelTransaccionGrande) {
                fields.modelTransaccionGrande.value = montoValue >= 900 ? "1" : "0";
            }
            if (fields.modelSaldoRestante) {
                fields.modelSaldoRestante.value = saldoRestante.toFixed(2);
            }
            if (fields.modelCompraRiesgosa) {
                const isRiskyCategory = String(tx.tipo_comercio || "") === "ecommerce" || String(tx.tipo_comercio || "") === "transferencia";
                const isInternational = String(tx.ubicacion || "") === "internacional";
                fields.modelCompraRiesgosa.value = (isRiskyCategory || isInternational) ? "1" : "0";
            }
            if (fields.modelRiesgoEdadMonto) {
                fields.modelRiesgoEdadMonto.value = (montoValue / 35).toFixed(4);
            }
            if (fields.modelDiaSemana) {
                fields.modelDiaSemana.value = String(diaSemana);
            }
            if (fields.modelMes) {
                fields.modelMes.value = String(mes);
            }
            if (fields.modelHora) {
                fields.modelHora.value = String(safeHour);
            }
            if (fields.modelTransaccionNocturna) {
                fields.modelTransaccionNocturna.value = (safeHour <= 5 || safeHour >= 23) ? "1" : "0";
            }

            if (fields.geoDisplay) {
                fields.geoDisplay.value = String(tx.geo_location_label ?? "");
            }
            if (fields.geoLabel) {
                fields.geoLabel.value = String(tx.geo_location_label ?? "");
            }
            if (fields.geoLat) {
                fields.geoLat.value = String(tx.geo_latitude ?? "");
            }
            if (fields.geoLng) {
                fields.geoLng.value = String(tx.geo_longitude ?? "");
            }
            if (fields.geoStatus) {
                fields.geoStatus.textContent = isEnglish
                    ? "Random real-time location assigned for simulation and map visualization."
                    : "Ubicación en tiempo real aleatoria asignada para simulación y visualización en el mapa.";
                fields.geoStatus.classList.remove("is-error");
                fields.geoStatus.classList.add("is-success");
            }
        };

        button.addEventListener("click", async () => {
            const originalLabel = button.textContent;
            const requestedCount = getRequestedSimulationCount();
            button.disabled = true;
            button.textContent = isEnglish
                ? `Generating (${requestedCount})...`
                : `Generando (${requestedCount})...`;

            try {
                const response = await fetch("/simular", {
                    method: "POST",
                    credentials: "same-origin",
                    headers: {
                        "Accept": "application/json",
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        cantidad: requestedCount
                    })
                });

                if (!response.ok) {
                    let errorMessage = "simulation_request_failed";
                    try {
                        const errorPayload = await response.json();
                        errorMessage = errorPayload.error || errorMessage;
                    } catch (_parseError) {
                        // Keep default message if response body is not JSON.
                    }
                    throw new Error(errorMessage);
                }

                const payload = await response.json();
                const summary = payload.resumen || {};
                const generatedTotal = Number(summary.total_generadas || 0);
                const fraudDetected = Number(summary.fraudes_detectados || 0);
                const lastItem = payload.ultima || {};
                const tx = lastItem.datos_transaccion || {};
                const result = lastItem.resultado_modelo || {};
                const generatedType = lastItem.tipo_generado || (isEnglish ? "unknown" : "desconocido");

                fillFields(tx);

                simulationStats = {
                    total: simulationStats.total + generatedTotal,
                    fraud: simulationStats.fraud + fraudDetected
                };
                renderStats(simulationStats);
                saveStats(simulationStats);

                const typeLabel = isEnglish ? "Generated type" : "Tipo generado";
                const riskLabel = isEnglish ? "Risk level" : "Nivel de riesgo";
                const probLabel = isEnglish ? "Fraud probability" : "Probabilidad de fraude";
                const recLabel = isEnglish ? "Recommendation" : "Recomendación";
                const generatedLabel = isEnglish ? "Created and saved" : "Creadas y guardadas";
                const batchFraudLabel = isEnglish ? "Detected frauds" : "Fraudes detectados";
                const batchRateLabel = isEnglish ? "Fraud rate" : "Tasa de fraude";
                const hint = isEnglish
                    ? "The form was auto-filled with the last generated transaction."
                    : "El formulario se autocompletó con la última transacción generada.";

                setOutput(
                    `${generatedLabel}: ${generatedTotal}\n${batchFraudLabel}: ${fraudDetected}\n${batchRateLabel}: ${summary.tasa_fraude ?? "0"}%\n${typeLabel}: ${generatedType}\n${riskLabel}: ${result.nivel_riesgo || "-"}\n${probLabel}: ${result.probabilidad_fraude ?? "-"}%\n${recLabel}: ${result.recomendacion || "-"}\n${hint}`,
                    false
                );
            } catch (error) {
                setOutput(
                    isEnglish
                        ? "Could not generate a simulated transaction. Please try again."
                        : "No se pudo generar una transacción simulada. Intenta nuevamente.",
                    true
                );
            } finally {
                button.disabled = false;
                button.textContent = originalLabel;
            }
        });
    });

    const animatedSelectors = [
        ".dashboard-header",
        ".module-banner",
        ".summary-card",
        ".panel-card",
        ".quick-link-card",
        ".metric-card",
        ".explain-row",
        ".kpi",
        ".score-box",
        ".sidebar-link",
        ".support-widget"
    ];

    const nodes = document.querySelectorAll(animatedSelectors.join(","));
    if (!nodes.length) {
        return;
    }

    nodes.forEach((node, index) => {
        node.classList.add("reveal-on-scroll");
        node.style.setProperty("--reveal-delay", `${Math.min(index * 55, 380)}ms`);
    });

    if (!("IntersectionObserver" in window)) {
        nodes.forEach((node) => node.classList.add("is-visible"));
        return;
    }

    const observer = new IntersectionObserver(
        (entries) => {
            entries.forEach((entry) => {
                if (!entry.isIntersecting) {
                    return;
                }

                entry.target.classList.add("is-visible");
                observer.unobserve(entry.target);
            });
        },
        {
            threshold: 0.12,
            rootMargin: "0px 0px -6% 0px"
        }
    );

    nodes.forEach((node) => observer.observe(node));

    const interactiveCards = document.querySelectorAll(
        ".summary-card, .panel-card, .metric-card, .kpi, .quick-link-card"
    );

    const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (!reducedMotion) {
        interactiveCards.forEach((card) => {
            card.addEventListener("pointermove", (event) => {
                const bounds = card.getBoundingClientRect();
                const x = event.clientX - bounds.left;
                const y = event.clientY - bounds.top;
                const centerX = bounds.width / 2;
                const centerY = bounds.height / 2;
                const rotateX = ((y - centerY) / centerY) * -2.8;
                const rotateY = ((x - centerX) / centerX) * 2.8;

                card.style.transform = `translateY(-4px) rotateX(${rotateX.toFixed(2)}deg) rotateY(${rotateY.toFixed(2)}deg)`;
                card.style.transformStyle = "preserve-3d";
            });

            card.addEventListener("pointerleave", () => {
                card.style.transform = "";
            });
        });
    }
})();
