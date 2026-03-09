#!/usr/bin/env python3
"""
AI-Powered Data Extractor using ChatGPT-like methodologies
Integrates with the existing MIDA system for enhanced extraction
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import openai

logger = logging.getLogger(__name__)

@dataclass
class ExtractionPrompt:
    """Structured prompt for data extraction"""
    system_prompt: str
    user_prompt: str
    examples: List[Dict[str, Any]]
    output_format: Dict[str, Any]

class AIExtractor:
    """AI-powered data extractor using ChatGPT methodologies"""

    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-3.5-turbo"):
        self.model = model
        self.api_key = api_key
        if api_key:
            openai.api_key = api_key

        # Predefined prompts for different extraction tasks
        self.prompts = self._initialize_prompts()

    def _initialize_prompts(self) -> Dict[str, ExtractionPrompt]:
        """Initialize extraction prompts"""

        # Mission extraction prompt
        mission_prompt = ExtractionPrompt(
            system_prompt="""Sei un esperto analista di documenti militari italiani.
            Il tuo compito è estrarre informazioni precise sulle missioni internazionali
            dai documenti ufficiali del Ministero della Difesa.""",

            user_prompt="""Analizza il seguente testo e estrai tutte le informazioni sulle missioni internazionali.

REGOLE:
- Estrai SOLO informazioni esplicite nel testo
- Se un'informazione non è chiara, usa "non specificato"
- Identifica il tipo di missione: ONU, UE, NATO, ITA, Bilateral
- Per i costi, converti in euro se necessario
- Per le date, usa formato YYYY-MM-DD

FORMATO OUTPUT:
{
  "missioni": [
    {
      "nome": "string",
      "paese": "string",
      "tipo": "string",
      "personale": "number",
      "costo": "number",
      "data_inizio": "string",
      "data_fine": "string",
      "confidenza": "number"
    }
  ],
  "statistiche": {
    "totale_missioni": "number",
    "totale_personale": "number",
    "totale_costi": "number",
    "paesi_coinvolti": "number"
  }
}

TESTO DA ANALIZZARE:
{text}""",

            examples=[
                {
                    "input": "Missione UNIFIL in Libano con 500 militari italiani, costo 50 milioni euro, periodo 2024-2025",
                    "output": {
                        "missioni": [{
                            "nome": "UNIFIL",
                            "paese": "Libano",
                            "tipo": "ONU",
                            "personale": 500,
                            "costo": 50000000,
                            "data_inizio": "2024-01-01",
                            "data_fine": "2025-12-31",
                            "confidenza": 0.95
                        }],
                        "statistiche": {
                            "totale_missioni": 1,
                            "totale_personale": 500,
                            "totale_costi": 50000000,
                            "paesi_coinvolti": 1
                        }
                    }
                }
            ],

            output_format={
                "type": "object",
                "properties": {
                    "missioni": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "nome": {"type": "string"},
                                "paese": {"type": "string"},
                                "tipo": {"type": "string"},
                                "personale": {"type": "number"},
                                "costo": {"type": "number"},
                                "data_inizio": {"type": "string"},
                                "data_fine": {"type": "string"},
                                "confidenza": {"type": "number"}
                            }
                        }
                    },
                    "statistiche": {
                        "type": "object",
                        "properties": {
                            "totale_missioni": {"type": "number"},
                            "totale_personale": {"type": "number"},
                            "totale_costi": {"type": "number"},
                            "paesi_coinvolti": {"type": "number"}
                        }
                    }
                }
            }
        )

        return {
            "missioni": mission_prompt
        }

    def extract_with_chain_of_thought(self, text: str, prompt_type: str = "missioni") -> Dict[str, Any]:
        """Extract data using chain-of-thought reasoning"""

        if not self.api_key:
            logger.warning("No API key provided, using fallback extraction")
            return self._fallback_extraction(text)

        prompt = self.prompts[prompt_type]

        # Chain-of-thought reasoning
        reasoning_prompt = f"""
{prompt.system_prompt}

RAGIONAMENTO STEP-BY-STEP:
1. Leggo attentamente il testo per identificare tutte le missioni
2. Per ogni missione, cerco informazioni su: nome, paese, personale, costi, date
3. Classifico il tipo di missione (ONU/UE/NATO/ITA/Bilateral)
4. Valido le informazioni per assicurarmi che siano esplicite nel testo
5. Calcolo le statistiche aggregate

{prompt.user_prompt.format(text=text[:4000])}  # Limit text length
"""

        try:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            # Configure retry strategy
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            HTTPAdapter(max_retries=retry_strategy)

            # Set timeout for requests
            openai.api_requestor.TIMEOUT_SECONDS = 30

            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt.system_prompt},
                    {"role": "user", "content": reasoning_prompt}
                ],
                temperature=0.1,  # Low temperature for consistent extraction
                max_tokens=2000,
                timeout=30  # 30 second timeout
            )

            result = json.loads(response.choices[0].message.content)
            logger.info(f"AI extraction completed: {len(result.get('missioni', []))} missions found")
            return result

        except openai.error.Timeout:
            logger.error("AI extraction timed out")
            return self._fallback_extraction(text)
        except openai.error.APIConnectionError as e:
            logger.error(f"AI extraction connection error: {str(e)}")
            return self._fallback_extraction(text)
        except openai.error.APIError as e:
            logger.error(f"AI extraction API error: {str(e)}")
            return self._fallback_extraction(text)
        except Exception as e:
            logger.error(f"AI extraction failed: {str(e)}")
            return self._fallback_extraction(text)

    def extract_with_few_shot(self, text: str, examples: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract data using few-shot learning approach"""

        if not self.api_key:
            return self._fallback_extraction(text)

        # Build few-shot prompt
        few_shot_prompt = "Estrai informazioni sulle missioni seguendo questi esempi:\n\n"

        for i, example in enumerate(examples[:3]):  # Use max 3 examples
            few_shot_prompt += f"ESEMPIO {i+1}:\n"
            few_shot_prompt += f"Input: {example['input']}\n"
            few_shot_prompt += f"Output: {json.dumps(example['output'], indent=2)}\n\n"

        few_shot_prompt += f"TESTO DA ANALIZZARE:\n{text[:3000]}"

        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Sei un esperto analista di documenti militari."},
                    {"role": "user", "content": few_shot_prompt}
                ],
                temperature=0.2
            )

            result = json.loads(response.choices[0].message.content)
            return result

        except Exception as e:
            logger.error(f"Few-shot extraction failed: {str(e)}")
            return self._fallback_extraction(text)

    def _fallback_extraction(self, text: str) -> Dict[str, Any]:
        """Fallback extraction using regex patterns when AI is not available"""

        # Basic regex patterns for extraction
        mission_patterns = {
            'UNIFIL': r'UNIFIL.*?(\d+).*?militar',
            'EUFOR': r'EUFOR.*?(\d+).*?soldat',
            'KFOR': r'KFOR.*?(\d+).*?personale',
            'NATO': r'NATO.*?(\d+).*?militar'
        }

        missions = []
        for mission_name, pattern in mission_patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                missions.append({
                    "nome": mission_name,
                    "paese": "non specificato",
                    "tipo": "ONU" if mission_name == "UNIFIL" else "NATO",
                    "personale": int(matches[0]) if matches[0].isdigit() else 0,
                    "costo": 0,
                    "data_inizio": "non specificato",
                    "data_fine": "non specificato",
                    "confidenza": 0.3
                })

        return {
            "missioni": missions,
            "statistiche": {
                "totale_missioni": len(missions),
                "totale_personale": sum(m.get('personale', 0) for m in missions),
                "totale_costi": sum(m.get('costo', 0) for m in missions),
                "paesi_coinvolti": len(set(m.get('paese', '') for m in missions))
            }
        }

    def extract_with_confidence_scoring(self, text: str) -> Dict[str, Any]:
        """Extract data with confidence scoring for each field"""

        if not self.api_key:
            return self._fallback_extraction(text)

        confidence_prompt = f"""
Analizza il testo e estrai informazioni con punteggi di confidenza (0-1):

TESTO: {text[:3000]}

Per ogni campo, assegna un punteggio di confidenza:
- 0.9-1.0: Informazione esplicita e chiara
- 0.7-0.8: Informazione implicita ma ragionevole
- 0.5-0.6: Informazione dedotta dal contesto
- 0.3-0.4: Informazione incerta
- 0.0-0.2: Informazione non trovata

FORMATO OUTPUT:
{{
  "missioni": [
    {{
      "nome": {{"value": "string", "confidence": 0.9}},
      "paese": {{"value": "string", "confidence": 0.8}},
      "personale": {{"value": "number", "confidence": 0.7}},
      "costo": {{"value": "number", "confidence": 0.6}}
    }}
  ]
}}
"""

        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Sei un analista esperto con capacità di valutare la confidenza delle informazioni estratte."},
                    {"role": "user", "content": confidence_prompt}
                ],
                temperature=0.1
            )

            return json.loads(response.choices[0].message.content)

        except Exception as e:
            logger.error(f"Confidence scoring extraction failed: {str(e)}")
            return self._fallback_extraction(text)

# Integration with existing system
def integrate_with_mida():
    """Integrate AI extractor with existing MIDA system"""

    # Example usage
    extractor = AIExtractor()

    # Test extraction
    sample_text = """
    Missione UNIFIL in Libano con 500 militari italiani,
    costo 50 milioni euro per il periodo 2024-2025.
    Operazione EUFOR in Bosnia con 200 soldati.
    """

    result = extractor.extract_with_chain_of_thought(sample_text)
    print(f"AI Extraction Result: {json.dumps(result, indent=2)}")

    return extractor

if __name__ == "__main__":
    integrate_with_mida()
