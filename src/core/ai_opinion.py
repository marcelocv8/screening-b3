"""
AI Daily Opinion using Google GenAI (nova SDK).
Analisa market breadth e gera parecer em português.
"""
import os
from typing import Dict, Any

try:
    from google import genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

def get_ai_opinion(breadth_data: Dict[str, Any], date_str: str) -> Dict[str, Any]:
    """
    Generate AI opinion on market regime using Gemini.
    Falls back to structured text if API is unavailable.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    
    if not api_key or not HAS_GENAI:
        return _fallback_opinion(breadth_data, date_str)
    
    try:
        client = genai.Client(api_key=api_key)
        prompt = _build_prompt(breadth_data, date_str)
        
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )
        text = response.text if response and hasattr(response, "text") else ""
        
        return {
            "date": date_str,
            "opinion": text.strip(),
            "allocation_score": breadth_data.get("allocation_score", 3),
            "regime": breadth_data.get("regime", "Neutro"),
            "source": "gemini-2.0-flash",
            "has_ai": True,
        }
    except Exception as e:
        print(f"[AI] Gemini API error: {e}")
        return _fallback_opinion(breadth_data, date_str)


def _build_prompt(breadth: Dict[str, Any], date: str) -> str:
    """Build the structured prompt for Gemini."""
    return f"""Você é um analista quantitativo sênior de mercado financeiro. Analise os dados abaixo e dê seu parecer diário em português.

DADOS DO MERCADO BRASILEIRO ({date}):
- Score de Alocação (1-5): {breadth.get('allocation_score', 'N/A')}
- Regime: {breadth.get('regime', 'N/A')}
- % de ações acima da MME50: {breadth.get('pct_above_sma50', 'N/A')}%
- % de ações acima da MME200: {breadth.get('pct_above_sma200', 'N/A')}%
- Tier S (momentum forte): {breadth.get('tier_s', 'N/A')}
- Tier A (momentum bom): {breadth.get('tier_a', 'N/A')}
- Novos breakouts detectados: {breadth.get('breakout_count', 'N/A')}
- VCPs em formação: {breadth.get('vcp_count', 'N/A')}
- Total de ativos analisados: {breadth.get('total_stocks', 'N/A')}

TAREFA:
1. Dê um parecer em 2-3 parágrafos sobre o regime de mercado atual.
2. Identifique riscos exógenos ou eventos que possam aumentar a incerteza.
3. Seja direto e pragmático. Não use linguagem corporativa genérica.
4. Conclua com uma recomendação de alocação em ações.

FORMATO:
- Parecer:
[seu texto]

- Riscos e Eventos:
[lista breve]

- Recomendação:
[alocação sugerida]
"""


def _fallback_opinion(breadth: Dict[str, Any], date: str) -> Dict[str, Any]:
    """Generate a text-based fallback when AI API is unavailable."""
    score = breadth.get("allocation_score", 3)
    regime = breadth.get("regime", "Neutro")
    
    opinions = {
        5: "Mercado com forte momentum técnico. Muitos ativos em tendência de alta com breakouts confirmados. É um bom momento para exposição total em ações de qualidade.",
        4: "Mercado positivo com setups técnicos construtivos. A maioria dos indicadores aponta para continuidade da tendência. Manter alocação moderada-alta.",
        3: "Mercado em equilíbrio. Indicadores mistos sugerem cautela. Selecionar apenas os melhores setups (Tier S/A) e manter posições menores.",
        2: "Mercado enfraquecendo. Poucos ativos em tendência positiva. Reduzir exposição e aumentar caixa ou hedge.",
        1: "Mercado em condições técnicas adversas. Proteger capital é prioridade. Mínima ou nenhuma exposição em ações.",
    }
    
    return {
        "date": date,
        "opinion": opinions.get(score, "Dados insuficientes para parecer."),
        "allocation_score": score,
        "regime": regime,
        "source": "fallback",
        "has_ai": False,
    }
