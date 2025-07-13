"""
Intelligent Data Extractor
Advanced NLP-based data extraction from PDF text
"""

import logging
import re
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import spacy
from pathlib import Path

class IntelligentDataExtractor:
    """Advanced data extractor using NLP techniques"""
    
    def __init__(self, language_model: str = "it_core_news_sm"):
        """
        Initialize the intelligent data extractor
        
        Args:
            language_model: spaCy language model to use
        """
        self.logger = logging.getLogger(__name__)
        
        try:
            # Load spaCy model
            self.nlp = spacy.load(language_model)
            self.logger.info(f"Loaded spaCy model: {language_model}")
        except OSError:
            self.logger.warning(f"Model {language_model} not found, using basic extraction")
            self.nlp = None
        
        # Enhanced patterns for mission data
        self.enhanced_patterns = {
            'mission_names': [
                r'(?:missione|mission|operazione|operation)\s+([A-Z][A-Z\s\-]+)',
                r'([A-Z][A-Z\s\-]+)\s+(?:mission|operazione)',
                r'(?:UNIFIL|KFOR|ISAF|EUTM|EUNAVFOR|EUBAM|EULEX|MINURSO|UNSMIS)',
                r'(?:EUFOR\s+ALTHEA|Enhanced\s+Vigilance|Forward\s+Land\s+Forces)'
            ],
            'countries': [
                r'(?:in|a|presso)\s+([A-Z][a-z\s]+)',
                r'(?:paese|country|stato|state)\s*[:\-]?\s*([A-Z][a-z\s]+)',
                r'(?:Libano|Kosovo|Afghanistan|Mali|Somalia|RCA|Bosnia|Serbia)'
            ],
            'personnel_numbers': [
                r'(\d+)\s+(?:militari|soldati|personale|personnel)',
                r'(?:personale|personnel)\s*[:\-]?\s*(\d+)',
                r'(\d+)\s+(?:unità|unità|units)',
                r'(?:totale|total)\s*[:\-]?\s*(\d+)'
            ],
            'costs': [
                r'(\d{1,3}(?:\.\d{3})*)\s*(?:euro|€|eur)',
                r'(?:costo|cost|spesa|expense)\s*[:\-]?\s*(\d{1,3}(?:\.\d{3})*)\s*(?:euro|€|eur)',
                r'(\d+)\s*(?:milioni|million)\s*(?:euro|€|eur)'
            ],
            'dates': [
                r'(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})',
                r'(\d{4}-\d{2}-\d{2})',
                r'(?:dal|from)\s+(\d{1,2}/\d{1,2}/\d{4})',
                r'(?:al|to)\s+(\d{1,2}/\d{1,2}/\d{4})'
            ],
            'organizations': [
                r'(?:ONU|UN|NATO|UE|EU|OSCE|OSCE)',
                r'(?:United\s+Nations|European\s+Union|North\s+Atlantic\s+Treaty\s+Organization)'
            ]
        }
        
        # Mission type classifiers
        self.mission_types = {
            'peacekeeping': ['peacekeeping', 'peace-keeping', 'peace keeping', 'mantenimento pace'],
            'training': ['training', 'addestramento', 'capacity building', 'formazione'],
            'security': ['security', 'sicurezza', 'stabilizzazione', 'stabilization'],
            'humanitarian': ['humanitarian', 'umanitario', 'assistenza', 'assistance'],
            'military': ['military', 'militare', 'combat', 'combattimento'],
            'civilian': ['civilian', 'civile', 'civil', 'civico']
        }
    
    def extract_structured_data(self, text: str) -> Dict[str, any]:
        """
        Extract structured data from text using multiple methods
        
        Args:
            text: Input text to analyze
            
        Returns:
            Dictionary with structured extracted data
        """
        if not text:
            return {}
        
        # Clean text
        cleaned_text = self._clean_text(text)
        
        # Extract data using different methods
        extracted_data = {
            'missions': self._extract_missions(cleaned_text),
            'countries': self._extract_countries(cleaned_text),
            'personnel': self._extract_personnel(cleaned_text),
            'costs': self._extract_costs(cleaned_text),
            'dates': self._extract_dates(cleaned_text),
            'organizations': self._extract_organizations(cleaned_text),
            'mission_types': self._classify_mission_types(cleaned_text),
            'entities': self._extract_entities(cleaned_text) if self.nlp else [],
            'confidence': 0.0
        }
        
        # Calculate overall confidence
        extracted_data['confidence'] = self._calculate_confidence(extracted_data)
        
        return extracted_data
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep important ones
        text = re.sub(r'[^\w\s\-\.\,\:\;\€\(\)]', ' ', text)
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text
    
    def _extract_missions(self, text: str) -> List[Dict]:
        """Extract mission information"""
        missions = []
        
        for pattern in self.enhanced_patterns['mission_names']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                mission_name = match.group(1) if match.groups() else match.group(0)
                missions.append({
                    'name': mission_name.strip(),
                    'confidence': 0.8,
                    'position': match.start()
                })
        
        # Remove duplicates and sort by confidence
        unique_missions = []
        seen_names = set()
        
        for mission in sorted(missions, key=lambda x: x['confidence'], reverse=True):
            if mission['name'] not in seen_names:
                unique_missions.append(mission)
                seen_names.add(mission['name'])
        
        return unique_missions
    
    def _extract_countries(self, text: str) -> List[Dict]:
        """Extract country information"""
        countries = []
        
        for pattern in self.enhanced_patterns['countries']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                country_name = match.group(1) if match.groups() else match.group(0)
                countries.append({
                    'name': country_name.strip(),
                    'confidence': 0.7,
                    'position': match.start()
                })
        
        return self._remove_duplicates(countries, 'name')
    
    def _extract_personnel(self, text: str) -> List[Dict]:
        """Extract personnel numbers"""
        personnel = []
        
        for pattern in self.enhanced_patterns['personnel_numbers']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    number = int(match.group(1))
                    personnel.append({
                        'number': number,
                        'confidence': 0.9,
                        'context': match.group(0),
                        'position': match.start()
                    })
                except (ValueError, IndexError):
                    continue
        
        return personnel
    
    def _extract_costs(self, text: str) -> List[Dict]:
        """Extract cost information"""
        costs = []
        
        for pattern in self.enhanced_patterns['costs']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    # Clean cost string
                    cost_str = match.group(1).replace('.', '').replace(',', '')
                    cost = float(cost_str)
                    costs.append({
                        'amount': cost,
                        'currency': 'EUR',
                        'confidence': 0.8,
                        'context': match.group(0),
                        'position': match.start()
                    })
                except (ValueError, IndexError):
                    continue
        
        return costs
    
    def _extract_dates(self, text: str) -> List[Dict]:
        """Extract date information"""
        dates = []
        
        for pattern in self.enhanced_patterns['dates']:
            matches = re.finditer(pattern, text)
            for match in matches:
                try:
                    date_str = match.group(1)
                    # Try to parse the date
                    parsed_date = self._parse_date(date_str)
                    if parsed_date:
                        dates.append({
                            'date': parsed_date,
                            'original': date_str,
                            'confidence': 0.7,
                            'position': match.start()
                        })
                except Exception:
                    continue
        
        return dates
    
    def _extract_organizations(self, text: str) -> List[Dict]:
        """Extract organization information"""
        organizations = []
        
        for pattern in self.enhanced_patterns['organizations']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                org_name = match.group(0)
                organizations.append({
                    'name': org_name.strip(),
                    'confidence': 0.9,
                    'position': match.start()
                })
        
        return self._remove_duplicates(organizations, 'name')
    
    def _classify_mission_types(self, text: str) -> List[Dict]:
        """Classify mission types based on keywords"""
        classifications = []
        
        for mission_type, keywords in self.mission_types.items():
            for keyword in keywords:
                if keyword.lower() in text.lower():
                    classifications.append({
                        'type': mission_type,
                        'keyword': keyword,
                        'confidence': 0.6
                    })
                    break  # One match per type is enough
        
        return classifications
    
    def _extract_entities(self, text: str) -> List[Dict]:
        """Extract named entities using spaCy"""
        if not self.nlp:
            return []
        
        doc = self.nlp(text)
        entities = []
        
        for ent in doc.ents:
            entities.append({
                'text': ent.text,
                'label': ent.label_,
                'start': ent.start_char,
                'end': ent.end_char,
                'confidence': 0.8
            })
        
        return entities
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string to date object"""
        try:
            # Try different date formats
            formats = [
                '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d',
                '%d/%m/%y', '%d-%m-%y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            
            return None
        except Exception:
            return None
    
    def _remove_duplicates(self, items: List[Dict], key: str) -> List[Dict]:
        """Remove duplicates from list of dictionaries"""
        seen = set()
        unique_items = []
        
        for item in items:
            if item[key] not in seen:
                unique_items.append(item)
                seen.add(item[key])
        
        return unique_items
    
    def _calculate_confidence(self, data: Dict) -> float:
        """Calculate overall confidence score"""
        total_items = 0
        total_confidence = 0.0
        
        for key, value in data.items():
            if key == 'confidence':
                continue
            
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and 'confidence' in item:
                        total_confidence += item['confidence']
                        total_items += 1
            elif isinstance(value, (int, float)):
                total_items += 1
                total_confidence += 0.5  # Default confidence for simple values
        
        return total_confidence / total_items if total_items > 0 else 0.0
    
    def process_pdf_results(self, pdf_results: List[Dict]) -> Dict[str, any]:
        """
        Process results from multiple PDFs
        
        Args:
            pdf_results: List of PDF extraction results
            
        Returns:
            Aggregated analysis results
        """
        aggregated_data = {
            'total_files': len(pdf_results),
            'successful_extractions': 0,
            'total_missions': 0,
            'total_personnel': 0,
            'total_costs': 0,
            'countries_found': set(),
            'organizations_found': set(),
            'mission_types': {},
            'file_details': []
        }
        
        for result in pdf_results:
            if 'error' in result:
                continue
            
            aggregated_data['successful_extractions'] += 1
            
            # Extract structured data
            structured_data = self.extract_structured_data(
                result['extracted_data']['full_text']
            )
            
            # Aggregate data
            aggregated_data['total_missions'] += len(structured_data.get('missions', []))
            aggregated_data['total_personnel'] += sum(
                p['number'] for p in structured_data.get('personnel', [])
            )
            aggregated_data['total_costs'] += sum(
                c['amount'] for c in structured_data.get('costs', [])
            )
            
            # Collect unique countries and organizations
            for country in structured_data.get('countries', []):
                aggregated_data['countries_found'].add(country['name'])
            
            for org in structured_data.get('organizations', []):
                aggregated_data['organizations_found'].add(org['name'])
            
            # Count mission types
            for mission_type in structured_data.get('mission_types', []):
                mission_type_name = mission_type['type']
                aggregated_data['mission_types'][mission_type_name] = \
                    aggregated_data['mission_types'].get(mission_type_name, 0) + 1
            
            # Store file details
            aggregated_data['file_details'].append({
                'filename': result['file'],
                'confidence': structured_data.get('confidence', 0.0),
                'missions_found': len(structured_data.get('missions', [])),
                'personnel_found': len(structured_data.get('personnel', [])),
                'costs_found': len(structured_data.get('costs', []))
            })
        
        # Convert sets to lists for JSON serialization
        aggregated_data['countries_found'] = list(aggregated_data['countries_found'])
        aggregated_data['organizations_found'] = list(aggregated_data['organizations_found'])
        
        return aggregated_data 