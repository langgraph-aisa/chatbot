"""
supervisor_jarvi.py - Módulo de supervisión determinista para JARVI 2.0.
VERSIÓN 2.4 – Si fuente_producto == "odoo", retorna allow sin evaluar reglas.
14AGO2026.
"""

import json
import re
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SupervisorJarvi:
    """Motor de supervisión determinista que valida y corrige las respuestas del agente."""

    def __init__(self, rules_path: str = "rules.json"):
        with open(rules_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        self.rules = self.config.get("rules", [])
        self.default_action = self.config.get("default_action", "allow")
        self._cache = {}

    def evaluate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # ================================================================
        # NUEVO: Si la fuente del producto es Odoo, permitir todo sin evaluar reglas
        # ================================================================
        if data.get("fuente_producto") == "odoo":
            logger.info("Producto verificado en Odoo, supervisor desactivado.")
            return {"decision": "allow", "rule_id": "ODOO-ALLOW", "message": "Fuente verificada"}

        # ================================================================
        # Si MICDP está activo, permitir todo sin evaluar reglas
        # ================================================================
        if data.get("contexto", {}).get("micdp_active", False):
            logger.info("MICDP activo: supervisor retorna allow sin evaluar reglas.")
            return {"decision": "allow", "rule_id": "MICDP-001", "message": "MICDP activo, reglas suspendidas"}

        # ================================================================
        # Preparar datos para evaluación de reglas (flujo normal)
        # ================================================================
        ctx = data.get("contexto", {})
        response = data.get("response", "")
        user_message = data.get("user_message", "")
        messages = data.get("messages", [])
        score = ctx.get("score_actual", 0.0)
        
        data["score"] = score
        data["product_tag"] = ctx.get("product_tag")
        data["tipo_producto"] = ctx.get("tipo_producto")
        data["topologia"] = ctx.get("topologia")
        data["nombre"] = ctx.get("nombre")
        data["telefono"] = ctx.get("whatsapp")
        data["ubicacion_need"] = ctx.get("ubicacion_need")
        data["vendedor_asignado"] = ctx.get("vendedor")
        data["escalation_mode"] = ctx.get("escalation_mode", False)
        data["authorization_response"] = ctx.get("authorization_response")
        data["conversation_end"] = ctx.get("conversation_end", False)
        data["dimensionamiento_exists"] = bool(ctx.get("dimensionamiento"))
        data["calculo_carga_completado"] = ctx.get("calculo_carga_completado", False)
        data["requisitos_pendientes"] = any(r.get("field") and ctx.get(r.get("field")) is None for r in ctx.get("requisitos", []))
        data["all_fields_collected"] = all([
            ctx.get("nombre"),
            ctx.get("whatsapp"),
            ctx.get("ubicacion_need")
        ])
        data["nombre_exists"] = bool(ctx.get("nombre"))
        data["telefono_exists"] = bool(ctx.get("whatsapp"))
        data["price_mentioned"] = bool(re.search(r'\d+\.?\d*\s*(GTQ|Q|USD|US\$|dólares|quetzales)', response))
        data["user_query_exists"] = bool(user_message)
        data["price_extractor_failed"] = data.get("price_extractor_failed", False)
        data["micdp_offered"] = ctx.get("micdp_offered", False)

        for rule in self.rules:
            if not self._condition_applies(rule, data):
                continue
            result = self._apply_rule(rule, data)
            if not result.get("passed", True):
                action = result.get("action", rule.get("requirement", {}).get("action", "block"))
                return {
                    "decision": action,
                    "rule_id": rule["id"],
                    "rule_name": rule["name"],
                    "message": result.get("message", rule.get("requirement", {}).get("message", "Regla violada")),
                    "modified_response": result.get("modified_response"),
                    "modified_context": result.get("modified_context")
                }
        return {"decision": "allow"}

    def _condition_applies(self, rule: Dict, data: Dict) -> bool:
        cond = rule.get("condition", {})
        for key, value in cond.items():
            if key == "output_type":
                if data.get("output_type") != value:
                    return False
            elif key == "input_type":
                if data.get("input_type") != value:
                    return False
            elif key == "product_tag_in":
                if data.get("product_tag") not in value:
                    return False
            elif key == "product_tag_range":
                tag = data.get("product_tag")
                if tag is None or not tag.isdigit():
                    return False
                tag_int = int(tag)
                if not (value[0] <= tag_int <= value[1]):
                    return False
            elif key == "topologia":
                if data.get("topologia") != value:
                    return False
            elif key == "tipo_producto":
                if data.get("tipo_producto") != value:
                    return False
            elif key == "score":
                score = data.get("score", 0.0)
                if value.startswith("<"):
                    max_val = float(value[1:])
                    if score >= max_val:
                        return False
                elif value.startswith(">"):
                    min_val = float(value[1:])
                    if score <= min_val:
                        return False
            elif key == "intent":
                if data.get("intent") != value:
                    return False
            elif key == "escalation_mode":
                if data.get("escalation_mode") != value:
                    return False
            elif key == "nombre":
                if data.get("nombre") != value:
                    return False
            elif key == "nombre_exists":
                if bool(data.get("nombre")) != value:
                    return False
            elif key == "telefono_exists":
                if bool(data.get("whatsapp")) != value:
                    return False
            elif key == "ubicacion_need":
                if data.get("ubicacion_need") != value:
                    return False
            elif key == "all_fields_collected":
                if data.get("all_fields_collected") != value:
                    return False
            elif key == "authorization_response":
                if data.get("authorization_response") != value:
                    return False
            elif key == "conversation_end":
                if data.get("conversation_end") != value:
                    return False
            elif key == "product_tag_exists":
                if bool(data.get("product_tag")) != value:
                    return False
            elif key == "user_technical":
                user_msg = data.get("user_message", "")
                technical_pattern = r'\b(W/m²|STC|NOCT|MPPT|PWM|VOC|ISC)\b'
                has_technical = bool(re.search(technical_pattern, user_msg, re.IGNORECASE))
                if has_technical != value:
                    return False
            elif key == "dimensionamiento_exists":
                if data.get("dimensionamiento_exists") != value:
                    return False
            elif key == "calculo_carga_completado":
                if data.get("calculo_carga_completado") != value:
                    return False
            elif key == "requisitos_pendientes":
                if data.get("requisitos_pendientes") != value:
                    return False
            elif key == "price_mentioned":
                if data.get("price_mentioned") != value:
                    return False
            elif key == "user_query_exists":
                if data.get("user_query_exists") != value:
                    return False
            elif key == "price_extractor_failed":
                if data.get("price_extractor_failed") != value:
                    return False
            elif key == "micdp_offered":
                if data.get("micdp_offered") != value:
                    return False
            elif key == "micdp_active":
                if data.get("micdp_active") != value:
                    return False
            # NUEVO: condición para fuente_producto (aunque ya la saltamos antes, por si acaso)
            elif key == "fuente_producto":
                if data.get("fuente_producto") != value:
                    return False
        return True

    def _apply_rule(self, rule: Dict, data: Dict) -> Dict:
        req = rule.get("requirement", {})
        rule_type = req.get("type")
        action = req.get("action", "block")
        result = {"passed": True}

        if rule_type == "regex_block":
            pattern = req.get("pattern")
            text = data.get("response", "")
            if re.search(pattern, text, re.IGNORECASE):
                return {"passed": False, "message": req.get("message"), "action": action}

        elif rule_type == "transform":
            func_name = req.get("function")
            text = data.get("response", "")
            if func_name == "remove_markdown":
                text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
                text = re.sub(r'\*(.*?)\*', r'\1', text)
                text = re.sub(r'__(.*?)__', r'\1', text)
                text = re.sub(r'_(.*?)_', r'\1', text)
                text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
                text = re.sub(r'^[\s]*[-*•]\s+', '', text, flags=re.MULTILINE)
                text = re.sub(r' +', ' ', text)
                text = re.sub(r'\s+([.,;:!?])', r'\1', text)
                text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
                result["modified_response"] = text.strip()
                return {"passed": True, "action": "rewrite", "message": req.get("message"), "modified_response": text}

        elif rule_type == "word_count_validate":
            text = data.get("response", "")
            words = text.split()
            min_w = req.get("min_words", 33)
            max_w = req.get("max_words", 66)
            if len(words) < min_w or len(words) > max_w:
                if len(words) > max_w:
                    truncated = " ".join(words[:max_w])
                    result["modified_response"] = truncated
                else:
                    return {"passed": True}
                return {"passed": True, "action": "rewrite", "message": req.get("message"), "modified_response": truncated}

        elif rule_type == "ontology_validation":
            response = data.get("response", "")
            try:
                from ontology import cargar_ontologia
                ontologia = cargar_ontologia()
                permitidos = set()
                for item in ontologia.values():
                    if isinstance(item, dict):
                        nombre = item.get("nombre", "").lower()
                        if nombre:
                            permitidos.add(nombre)
                        for kw in item.get("keywords", []):
                            if kw:
                                permitidos.add(kw.lower())
                entidades = re.findall(r'\b[A-Z][a-z]+(?:[\s-][A-Z][a-z]+)*\b', response)
                entidades_no_validas = []
                for entidad in entidades:
                    if entidad.lower() not in permitidos and len(entidad) > 3:
                        entidades_no_validas.append(entidad)
                if entidades_no_validas:
                    mensaje = req.get("message", "Para conocer los detalles específicos, lo mejor es que un asesor de AISA Solar le brinde una cotización personalizada.")
                    return {"passed": False, "message": mensaje, "action": "rewrite", "modified_response": mensaje}
            except Exception as e:
                logger.warning(f"Ontología no disponible para validación: {e}")
            return {"passed": True}

        elif rule_type == "ontology_allowlist":
            response = data.get("response", "")
            try:
                from ontology import cargar_ontologia
                ontologia = cargar_ontologia()
                permitidos = set()
                for item in ontologia.values():
                    if isinstance(item, dict):
                        nombre = item.get("nombre", "").lower()
                        if nombre:
                            permitidos.add(nombre)
                        for kw in item.get("keywords", []):
                            if kw:
                                permitidos.add(kw.lower())
                palabras = re.findall(r'\b[A-Za-zÁÉÍÓÚáéíóúñÑ\-]+\b', response)
                for palabra in palabras:
                    if len(palabra) > 3 and palabra.lower() not in permitidos:
                        return {"passed": False, "message": req.get("message"), "action": "rewrite"}
            except Exception as e:
                logger.warning(f"Ontología no disponible para allowlist: {e}")
            return {"passed": True}

        elif rule_type == "regex_block_with_exception":
            pattern = req.get("pattern")
            text = data.get("response", "")
            exception_source = req.get("exception_source")
            exception_fields = req.get("exception_fields", [])
            matches = re.findall(pattern, text, re.IGNORECASE)
            if not matches:
                return {"passed": True}
            try:
                from ontology import cargar_ontologia
                ontologia = cargar_ontologia()
                excepciones = set()
                for item in ontologia.values():
                    if isinstance(item, dict):
                        for field in exception_fields:
                            value = item.get(field, [])
                            if isinstance(value, str):
                                excepciones.add(value.lower())
                            elif isinstance(value, list):
                                for v in value:
                                    if v:
                                        excepciones.add(v.lower())
                for match in matches:
                    if match.lower() not in excepciones:
                        return {"passed": False, "message": req.get("message"), "action": req.get("action", "rewrite")}
            except Exception as e:
                logger.warning(f"Ontología no disponible para excepción: {e}")
            return {"passed": True}

        elif rule_type == "ontology_anchor":
            return {"passed": True}

        elif rule_type == "product_category_match":
            return {"passed": True}

        elif rule_type == "trigger_escalation":
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "force_fallback",
                "modified_context": {"escalation_mode": True}
            }

        elif rule_type == "trigger_micdp":
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "trigger_micdp",
                "modified_context": {"micdp_accepted": True, "micdp_offered": False}
            }

        elif rule_type == "interrupt_micdp":
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "interrupt_micdp",
                "modified_context": {"micdp_interrupted": True}
            }

        elif rule_type == "human_handoff":
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "human_handoff",
                "modified_context": {"conversation_end": True}
            }

        elif rule_type == "ask_field":
            field = req.get("field")
            question = req.get("question")
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite",
                "modified_response": question
            }

        elif rule_type == "ask_authorization":
            question = req.get("question")
            telefono = data.get("whatsapp", "su teléfono")
            producto = data.get("product_tag", "su producto")
            question = question.replace("{telefono}", telefono).replace("{producto}", producto)
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite",
                "modified_response": question,
                "modified_context": {"authorization_asked": True}
            }

        elif rule_type == "finalize_closure":
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "end_conversation",
                "modified_response": req.get("message"),
                "modified_context": {"conversation_end": True}
            }

        elif rule_type == "offer_alternative":
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite",
                "modified_response": req.get("message")
            }

        elif rule_type == "intent_detection":
            user_msg = data.get("user_message", "")
            keywords = req.get("keywords", [])
            if any(k in user_msg.lower() for k in keywords):
                if "trigger_micdp" in req.get("action", ""):
                    return {
                        "passed": False,
                        "message": req.get("message"),
                        "action": "trigger_micdp",
                        "modified_context": {"micdp_accepted": True}
                    }
                return {
                    "passed": False,
                    "message": req.get("message"),
                    "action": "force_closure",
                    "modified_context": {"intent": "buy"}
                }
            return {"passed": True}

        elif rule_type == "inject_questions":
            questions = req.get("questions", [])
            response = data.get("response", "")
            new_response = response + "\n\n" + "\n".join(questions)
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite",
                "modified_response": new_response
            }

        elif rule_type == "force_topolgia":
            value = req.get("value")
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite_context",
                "modified_context": {"topologia": value}
            }

        elif rule_type == "ask_requirement":
            field = req.get("field")
            question = req.get("question")
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite",
                "modified_response": question
            }

        elif rule_type == "trigger_dimensionamiento":
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite_context",
                "modified_context": {"calculo_carga_completado": False, "dimensionamiento_triggered": True}
            }

        elif rule_type == "ask_requirements":
            requisitos = data.get("requisitos", [])
            preguntas = []
            for req in requisitos:
                if req.get("field") and data.get("contexto", {}).get(req.get("field")) is None:
                    preguntas.append(req.get("question", f"¿Cuál es su {req.get('field')}?"))
            if preguntas:
                question = "\n".join(preguntas)
                return {
                    "passed": False,
                    "message": req.get("message"),
                    "action": "rewrite",
                    "modified_response": question
                }
            return {"passed": True}

        elif rule_type == "confirm_phone":
            telefono = data.get("whatsapp", "")
            message = req.get("message", "").replace("{telefono}", telefono)
            return {
                "passed": False,
                "message": req.get("message"),
                "action": "rewrite",
                "modified_response": message
            }

        elif rule_type == "regex_require":
            pattern = req.get("pattern")
            response = data.get("response", "")
            if not re.search(pattern, response, re.IGNORECASE):
                new_response = response + " " + req.get("message", "")
                return {
                    "passed": False,
                    "message": req.get("message"),
                    "action": "rewrite",
                    "modified_response": new_response
                }
            return {"passed": True}

        elif rule_type == "token_limit":
            max_tokens = req.get("max_tokens", 120000)
            messages = data.get("messages", [])
            try:
                import tiktoken
                enc = tiktoken.get_encoding("cl100k_base")
                total_tokens = 0
                for msg in messages:
                    content = msg.content if hasattr(msg, 'content') else str(msg)
                    total_tokens += len(enc.encode(content))
                if total_tokens > max_tokens:
                    truncated = messages[-10:]
                    return {
                        "passed": False,
                        "message": req.get("message"),
                        "action": "truncate",
                        "modified_context": {"messages": truncated}
                    }
            except Exception as e:
                logger.warning(f"No se pudo contar tokens con tiktoken: {e}")
            return {"passed": True}

        elif rule_type == "price_fallback":
            response = data.get("response", "")
            if re.search(r'\d+\.?\d*\s*(GTQ|Q|USD)', response):
                new_response = re.sub(r'\d+\.?\d*\s*(GTQ|Q|USD)', 'disponible bajo consulta', response)
                return {
                    "passed": False,
                    "message": req.get("message"),
                    "action": "rewrite",
                    "modified_response": new_response
                }
            return {"passed": True}

        elif rule_type == "fallback_local":
            logger.info("Fallback a memoria local activado.")
            return {"passed": True}

        elif rule_type == "log_alert":
            logger.warning("ALERTA: Latencia del LLM > 10 segundos.")
            return {"passed": True}

        elif rule_type == "allow":
            return {"passed": True}

        return {"passed": True}
