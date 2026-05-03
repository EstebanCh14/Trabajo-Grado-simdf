(function () {
    const supportWidget = document.getElementById("supportWidget");
    if (!supportWidget) {
        return;
    }

    const toggleButton = document.getElementById("supportToggle");
    const panel = document.getElementById("supportPanel");
    const messages = document.getElementById("supportMessages");
    const input = document.getElementById("supportInput");
    const sendButton = document.getElementById("supportSend");
    const quickContainer = supportWidget.querySelector(".support-quick");
    const quickButtons = () => supportWidget.querySelectorAll(".support-quick-btn");
    const currentLanguage = (document.body?.dataset.language || document.documentElement.lang || "es").toLowerCase();
    const isEnglish = currentLanguage.startsWith("en");

    if (toggleButton) {
        toggleButton.innerHTML = '<span class="support-avatar-icon" aria-hidden="true"></span>';
        toggleButton.setAttribute("aria-label", isEnglish ? "Help" : "Ayuda");
        toggleButton.setAttribute("title", isEnglish ? "Help" : "Ayuda");
        toggleButton.setAttribute("data-hover-msg", isEnglish ? "SIMDF Help Center" : "Centro de ayuda SIMDF");
    }

    const defaultQuickQuestions = isEnglish
        ? [
            { label: "How to analyze?", question: "how to analyze" },
            { label: "View history", question: "view history" },
            { label: "Risk levels", question: "risk levels" },
            { label: "Model metrics", question: "model metrics" },
            { label: "SHAP explainability", question: "shap explainability" },
            { label: "Admin role", question: "admin role" },
        ]
        : [
            { label: "¿Cómo analizar?", question: "como analizar" },
            { label: "Ver historial", question: "ver historial" },
            { label: "Niveles de riesgo", question: "que significa riesgo" },
            { label: "Métricas del modelo", question: "metricas del modelo" },
            { label: "Explicabilidad SHAP", question: "explicabilidad shap" },
            { label: "Rol administrador", question: "rol administrador" },
        ];

    if (quickContainer) {
        const existing = new Set(
            Array.from(quickContainer.querySelectorAll(".support-quick-btn")).map(
                (button) => (button.getAttribute("data-question") || "").toLowerCase().trim()
            )
        );

        defaultQuickQuestions.forEach((item) => {
            if (existing.has(item.question)) {
                return;
            }

            const button = document.createElement("button");
            button.type = "button";
            button.className = "support-quick-btn";
            button.setAttribute("data-question", item.question);
            button.textContent = item.label;
            quickContainer.appendChild(button);
        });
    }

    const responses = isEnglish
        ? {
            "how to analyze": "To analyze a transaction, go to the main module, fill in the fields and click 'Analyze risk'.",
            "risk levels": "High risk suggests block, Medium risk suggests manual review, and Low risk suggests approval.",
            "view history": "You can review all queries from the 'History' section.",
            "admin role": "The administrator can view global metrics and all user queries.",
            "model metrics": "In Model metrics you can compare accuracy, precision, recall, F1 and ROC curve.",
            "shap explainability": "Explainability shows which variables influenced each model decision the most.",
            "date filter": "In History you can filter by Today, Last 7 days or Last month.",
            "export": "You can export results using 'Export CSV' and 'Download PDF report'.",
            "hello": "Hi 👋 I am the SIMDF assistant. How can I help you today?"
        }
        : {
            "como analizar": "Para analizar una transacción, ve al módulo principal, completa los campos y pulsa 'Analizar Riesgo'.",
            "que significa riesgo": "Riesgo Alto sugiere bloquear, Riesgo Medio revisar manualmente y Riesgo Bajo aprobar.",
            "ver historial": "Puedes revisar todas las consultas desde el botón 'Historial' en la parte superior.",
            "rol administrador": "El administrador ve métricas globales y consultas de todos los usuarios.",
            "metricas del modelo": "En Métricas del modelo puedes comparar accuracy, precision, recall, F1 y la curva ROC.",
            "explicabilidad shap": "La explicabilidad muestra qué variables influyeron más en la decisión del modelo para cada evaluación.",
            "filtro por fecha": "En Historial puedes filtrar por Hoy, Últimos 7 días o Último mes para análisis puntual.",
            "exportar": "Puedes exportar resultados con los botones 'Exportar a CSV' y 'Descargar reporte PDF'.",
            "hola": "Hola 👋 Soy el asistente de SIMDF. ¿En qué te ayudo hoy?"
        };

    const addMessage = (text, from) => {
        const bubble = document.createElement("div");
        bubble.className = `support-msg ${from === "bot" ? "bot" : "user"}`;
        bubble.textContent = text;
        messages.appendChild(bubble);
        messages.scrollTop = messages.scrollHeight;
    };

    const getBotReply = (text) => {
        const normalized = text.toLowerCase().trim();

        if (!normalized) {
            return isEnglish ? "Tell me your question and I will guide you step by step." : "Cuéntame tu duda y te guío paso a paso.";
        }

        const matchedKey = Object.keys(responses).find((key) => normalized.includes(key));
        if (matchedKey) {
            return responses[matchedKey];
        }

        return isEnglish
            ? "I can help with: analysis, risk levels, history, data export, and user roles."
            : "Puedo ayudarte con: análisis, niveles de riesgo, historial o roles de usuario.";
    };

    const sendUserMessage = (text) => {
        addMessage(text, "user");
        const reply = getBotReply(text);
        window.setTimeout(() => addMessage(reply, "bot"), 240);
    };

    const togglePanel = () => {
        const opened = panel.classList.toggle("open");
        toggleButton.setAttribute("aria-expanded", opened ? "true" : "false");
        if (opened) {
            input.focus();
        }
    };

    toggleButton.addEventListener("click", togglePanel);

    sendButton.addEventListener("click", () => {
        const text = input.value;
        if (!text.trim()) {
            return;
        }
        sendUserMessage(text);
        input.value = "";
    });

    input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
            event.preventDefault();
            sendButton.click();
        }
    });

    quickButtons().forEach((button) => {
        button.addEventListener("click", () => {
            const question = button.getAttribute("data-question") || "";
            sendUserMessage(question);
        });
    });
})();
