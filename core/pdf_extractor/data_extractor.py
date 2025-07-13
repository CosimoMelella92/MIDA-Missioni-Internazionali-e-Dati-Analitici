"""
Intelligent Data Extractor
Advanced NLP-based data extraction from PDF text
"""

import logging
import re
import json
import os
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
import spacy
from pathlib import Path

# Import AI extractor
try:
    from .ai_extractor import AIExtractor
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False
    AIExtractor = None

class IntelligentDataExtractor:
    """Advanced data extractor using NLP techniques"""
    
    def __init__(self, language_model: str = "it_core_news_sm", use_ai: bool = False, ai_api_key: str = None):
        """
        Initialize the intelligent data extractor
        
        Args:
            language_model: spaCy language model to use
            use_ai: Whether to use AI extraction
            ai_api_key: API key for AI extraction
        """
        self.logger = logging.getLogger(__name__)
        
        # Initialize AI extractor if requested
        self.ai_extractor = None
        if use_ai and AI_AVAILABLE:
            try:
                self.ai_extractor = AIExtractor(api_key=ai_api_key)
                self.logger.info("AI extractor initialized")
            except Exception as e:
                self.logger.warning(f"Failed to initialize AI extractor: {str(e)}")
                self.ai_extractor = None
        
        # Check if spaCy should be disabled for performance
        if os.environ.get('DISABLE_SPACY', 'false').lower() == 'true':
            self.logger.info("spaCy disabled for performance")
            self.nlp = None
        else:
            try:
                # Load spaCy model
                self.nlp = spacy.load(language_model)
                # Increase max length for large documents
                self.nlp.max_length = 5000000  # 5M characters
                self.logger.info(f"Loaded spaCy model: {language_model}")
            except OSError:
                self.logger.warning(f"Model {language_model} not found, using basic extraction")
                self.nlp = None
        
        # Enhanced patterns for mission data
        self.enhanced_patterns = {
            'mission_names': [
                # Specific mission names only
                r'\b(?:UNIFIL|KFOR|ISAF|EUTM|EUNAVFOR|EUBAM|EULEX|MINURSO|UNSMIS|EUFOR\s+ALTHEA)\b',
                # Mission patterns with context
                r'(?:missione|mission)\s+(?:in|a|presso)\s+([A-Z][a-z\s]+)',
                r'(?:operazione|operation)\s+(?:in|a|presso)\s+([A-Z][a-z\s]+)'
            ],
            'countries': [
                # Specific countries mentioned in Italian missions
                r'\b(?:Libano|Kosovo|Afghanistan|Mali|Somalia|RCA|Bosnia|Serbia|Iraq|Libia|Sudan|Ciad)\b',
                # Country with context
                r'(?:in|a|presso)\s+(?:il\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'(?:paese|country|stato|state)\s*[:\-]?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
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
        
        # Try AI extraction first if available
        if self.ai_extractor:
            try:
                self.logger.info("Attempting AI extraction...")
                ai_result = self.ai_extractor.extract_with_chain_of_thought(cleaned_text)
                if ai_result and ai_result.get('missioni'):
                    self.logger.info(f"AI extraction successful: {len(ai_result.get('missioni', []))} missions found")
                    return self._convert_ai_result_to_standard_format(ai_result)
            except Exception as e:
                self.logger.warning(f"AI extraction failed: {str(e)}, falling back to NLP")
        
        # Check for fast mode
        fast_mode = os.environ.get('FAST_MODE', 'false').lower() == 'true'
        
        # Handle very long texts by chunking (reduced threshold in fast mode)
        chunk_threshold = 500000 if fast_mode else 2000000  # 500K vs 2M
        if len(cleaned_text) > chunk_threshold:
            self.logger.info(f"Text too long ({len(cleaned_text)} chars), using chunking")
            return self._extract_from_chunks(cleaned_text)
        
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
        
        # Log extraction results for debugging
        self.logger.info(f"Extraction results: {len(extracted_data.get('missions', []))} missions, "
                        f"{len(extracted_data.get('countries', []))} countries, "
                        f"{len(extracted_data.get('personnel', []))} personnel entries, "
                        f"confidence: {extracted_data['confidence']:.2f}")
        
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
        
        # Known mission keywords for validation
        mission_keywords = ['missione', 'operazione', 'peacekeeping', 'stabilizzazione', 'addestramento', 'mission', 'operation']
        
        # Enhanced patterns for mission detection
        mission_patterns = [
            r'\b(?:missione|operazione)\s+([A-Z][A-Z\s]{2,30})\b',
            r'\b([A-Z]{2,10})\s+(?:missione|operazione)\b',
            r'\b(UNIFIL|KFOR|ISAF|EUTM|EUNAVFOR|EUBAM|EULEX|MINURSO|UNSMIS)\b',
            r'\b(?:missione|operazione)\s+(?:in|a|per)\s+([A-Z][a-z\s]{2,20})\b',
            r'\b([A-Z][a-z\s]{2,20})\s+(?:missione|operazione)\b'
        ]
        
        self.logger.info(f"Searching for missions in text of {len(text)} characters")
        
        for pattern in mission_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                mission_name = match.group(1) if match.groups() else match.group(0)
                mission_name = mission_name.strip()
                
                # Validate mission name
                if len(mission_name) < 3 or len(mission_name) > 50:
                    continue
                
                # Check if it's a real mission (contains mission keywords or is a known acronym)
                context_text = text[max(0, match.start()-100):match.end()+100].lower()
                is_valid = any(keyword in context_text for keyword in mission_keywords)
                is_known_acronym = any(acronym in mission_name.upper() for acronym in ['UNIFIL', 'KFOR', 'ISAF', 'EUTM', 'EUNAVFOR', 'EUBAM', 'EULEX', 'MINURSO', 'UNSMIS'])
                
                if is_valid or is_known_acronym:
                    confidence = 0.9 if is_known_acronym else 0.7
                    missions.append({
                        'name': mission_name,
                        'confidence': confidence,
                        'position': match.start(),
                        'context': context_text[:200]  # Add context for debugging
                    })
                    self.logger.info(f"Found mission: {mission_name} (confidence: {confidence})")
        
        # Also look for common mission patterns in the text
        common_missions = [
            'UNIFIL', 'KFOR', 'ISAF', 'EUTM', 'EUNAVFOR', 'EUBAM', 'EULEX', 'MINURSO', 'UNSMIS',
            'Missione Libano', 'Missione Kosovo', 'Missione Afghanistan', 'Missione Mali',
            'Operazione Libano', 'Operazione Kosovo', 'Operazione Afghanistan', 'Operazione Mali'
        ]
        
        for mission in common_missions:
            if mission.lower() in text.lower():
                missions.append({
                    'name': mission,
                    'confidence': 0.8,
                    'position': text.lower().find(mission.lower()),
                    'context': 'Common mission pattern'
                })
                self.logger.info(f"Found common mission: {mission}")
        
        # Remove duplicates and limit results
        unique_missions = []
        seen_names = set()
        
        for mission in sorted(missions, key=lambda x: x['confidence'], reverse=True):
            if mission['name'] not in seen_names and len(unique_missions) < 20:  # Limit to 20 missions max
                unique_missions.append(mission)
                seen_names.add(mission['name'])
        
        self.logger.info(f"Total unique missions found: {len(unique_missions)}")
        return unique_missions
    
    def _convert_ai_result_to_standard_format(self, ai_result: Dict[str, Any]) -> Dict[str, Any]:
        """Convert AI extraction result to standard format"""
        try:
            missions = ai_result.get('missioni', [])
            stats = ai_result.get('statistiche', {})
            
            # Convert missions to standard format
            standard_missions = []
            for mission in missions:
                standard_missions.append({
                    'name': mission.get('nome', ''),
                    'country': mission.get('paese', ''),
                    'type': mission.get('tipo', ''),
                    'personnel': mission.get('personale', 0),
                    'cost': mission.get('costo', 0),
                    'start_date': mission.get('data_inizio', ''),
                    'end_date': mission.get('data_fine', ''),
                    'confidence': mission.get('confidenza', 0.0)
                })
            
            return {
                'missions': standard_missions,
                'countries': list(set([m.get('country', '') for m in standard_missions if m.get('country')])),
                'personnel': [{'value': m.get('personnel', 0), 'confidence': m.get('confidence', 0.0)} for m in standard_missions],
                'costs': [{'value': m.get('cost', 0), 'confidence': m.get('confidence', 0.0)} for m in standard_missions],
                'dates': [{'value': m.get('start_date', ''), 'confidence': m.get('confidence', 0.0)} for m in standard_missions if m.get('start_date')],
                'organizations': list(set([m.get('type', '') for m in standard_missions if m.get('type')])),
                'mission_types': [],
                'entities': [],
                'confidence': sum([m.get('confidence', 0.0) for m in standard_missions]) / len(standard_missions) if standard_missions else 0.0,
                'ai_extracted': True,
                'total_missions': stats.get('totale_missioni', len(standard_missions)),
                'total_personnel': stats.get('totale_personale', sum([m.get('personnel', 0) for m in standard_missions])),
                'total_costs': stats.get('totale_costi', sum([m.get('cost', 0) for m in standard_missions])),
                'countries_involved': stats.get('paesi_coinvolti', len(set([m.get('country', '') for m in standard_missions if m.get('country')])))
            }
        except Exception as e:
            self.logger.error(f"Error converting AI result: {str(e)}")
            return {}
    
    def _extract_countries(self, text: str) -> List[Dict]:
        """Extract country information"""
        countries = []
        
        # Known countries for Italian missions
        known_countries = {
            'libano', 'kosovo', 'afghanistan', 'mali', 'somalia', 'rca', 'bosnia', 'serbia',
            'iraq', 'libia', 'sudan', 'ciad', 'somalia', 'yemen', 'siria', 'niger'
        }
        
        for pattern in self.enhanced_patterns['countries']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                country_name = match.group(1) if match.groups() else match.group(0)
                country_name = country_name.strip().lower()
                
                # Validate country name
                if len(country_name) < 3 or len(country_name) > 30:
                    continue
                
                # Check if it's a known country or has mission context
                is_known = country_name in known_countries
                has_context = any(keyword in text[max(0, match.start()-50):match.end()+50].lower() 
                                for keyword in ['missione', 'operazione', 'paese', 'stato'])
                
                if is_known or has_context:
                    countries.append({
                        'name': country_name.title(),
                        'confidence': 0.9 if is_known else 0.6,
                        'position': match.start()
                    })
        
        # Remove duplicates and limit results
        unique_countries = self._remove_duplicates(countries, 'name')
        return unique_countries[:15]  # Limit to 15 countries max
    
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
                        'confidence': 0.8,
                        'context': match.group(0),
                        'position': match.start()
                    })
                except (ValueError, IndexError, AttributeError):
                    # Skip invalid cost entries
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
        
        # Skip entity extraction in fast mode
        fast_mode = os.environ.get('FAST_MODE', 'false').lower() == 'true'
        if fast_mode:
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
        
        # Weight different types of data
        weights = {
            'missions': 0.3,
            'countries': 0.2,
            'personnel': 0.2,
            'costs': 0.15,
            'organizations': 0.15
        }
        
        for key, value in data.items():
            if key == 'confidence':
                continue
            
            if isinstance(value, list) and value:
                weight = weights.get(key, 0.1)
                avg_confidence = sum(item.get('confidence', 0) for item in value) / len(value)
                total_confidence += avg_confidence * weight
                total_items += 1
            elif isinstance(value, (int, float)) and value > 0:
                total_items += 1
                total_confidence += 0.3  # Lower default confidence
        
        confidence = total_confidence / total_items if total_items > 0 else 0.0
        
        # Cap confidence at realistic levels
        return min(confidence, 0.85)  # Maximum 85% confidence
    
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
            # Handle different result structures
            if 'full_text' in result:
                text = result['full_text']
            elif 'extracted_data' in result and 'full_text' in result['extracted_data']:
                text = result['extracted_data']['full_text']
            else:
                # Try to find text in the result structure
                text = result.get('text', '') or result.get('content', '') or ''
                if not text and 'pages' in result:
                    # Combine text from all pages
                    text = '\n'.join([page.get('text', '') for page in result['pages']])
            
            structured_data = self.extract_structured_data(text)
            
            # Aggregate data
            aggregated_data['total_missions'] += len(structured_data.get('missions', []))
            aggregated_data['total_personnel'] += sum(
                p.get('number', 0) for p in structured_data.get('personnel', [])
            )
            aggregated_data['total_costs'] += sum(
                c.get('amount', 0) for c in structured_data.get('costs', [])
            )
            
            # Collect unique countries and organizations
            for country in structured_data.get('countries', []):
                if 'name' in country:
                    aggregated_data['countries_found'].add(country['name'])
            
            for org in structured_data.get('organizations', []):
                if 'name' in org:
                    aggregated_data['organizations_found'].add(org['name'])
            
            # Count mission types
            for mission_type in structured_data.get('mission_types', []):
                if 'type' in mission_type:
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
    
    def _extract_from_chunks(self, text: str) -> Dict[str, any]:
        """Extract data from long text by processing in chunks"""
        # Use smaller chunks in fast mode
        fast_mode = os.environ.get('FAST_MODE', 'false').lower() == 'true'
        chunk_size = 500000 if fast_mode else 1000000  # 500K vs 1M characters per chunk
        chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
        
        self.logger.info(f"Processing {len(chunks)} chunks of ~{chunk_size} characters each")
        
        # Extract from each chunk
        all_missions = []
        all_countries = []
        all_personnel = []
        all_costs = []
        all_dates = []
        all_organizations = []
        all_mission_types = []
        all_entities = []
        
        for i, chunk in enumerate(chunks):
            self.logger.info(f"Processing chunk {i+1}/{len(chunks)}")
            
            # Extract from this chunk
            missions = self._extract_missions(chunk)
            countries = self._extract_countries(chunk)
            personnel = self._extract_personnel(chunk)
            costs = self._extract_costs(chunk)
            dates = self._extract_dates(chunk)
            organizations = self._extract_organizations(chunk)
            mission_types = self._classify_mission_types(chunk)
            entities = self._extract_entities(chunk) if self.nlp else []
            
            # Merge results
            all_missions.extend(missions)
            all_countries.extend(countries)
            all_personnel.extend(personnel)
            all_costs.extend(costs)
            all_dates.extend(dates)
            all_organizations.extend(organizations)
            all_mission_types.extend(mission_types)
            all_entities.extend(entities)
        
        # Remove duplicates
        all_missions = self._remove_duplicates(all_missions, 'name')
        all_countries = self._remove_duplicates(all_countries, 'name')
        all_organizations = self._remove_duplicates(all_organizations, 'name')
        all_mission_types = self._remove_duplicates(all_mission_types, 'type')
        
        # Merge personnel and costs (sum values)
        merged_personnel = self._merge_numerical_data(all_personnel, 'number')
        merged_costs = self._merge_numerical_data(all_costs, 'amount')
        
        extracted_data = {
            'missions': all_missions,
            'countries': all_countries,
            'personnel': merged_personnel,
            'costs': merged_costs,
            'dates': all_dates,
            'organizations': all_organizations,
            'mission_types': all_mission_types,
            'entities': all_entities,
            'confidence': 0.0
        }
        
        # Calculate overall confidence
        extracted_data['confidence'] = self._calculate_confidence(extracted_data)
        
        return extracted_data
    
    def _merge_numerical_data(self, data_list: List[Dict], key: str) -> List[Dict]:
        """Merge numerical data by summing values"""
        if not data_list:
            return []
        
        merged = {}
        for item in data_list:
            value = item.get(key, 0)
            context = item.get('context', '')
            
            if context not in merged:
                merged[context] = {'value': value, 'confidence': item.get('confidence', 0)}
            else:
                merged[context]['value'] += value
                merged[context]['confidence'] = max(merged[context]['confidence'], item.get('confidence', 0))
        
        # Return with the correct key based on the input key
        if key == 'amount':
            return [{'amount': v['value'], 'confidence': v['confidence'], 'context': k} 
                    for k, v in merged.items()]
        else:
            return [{'number': v['value'], 'confidence': v['confidence'], 'context': k} 
                    for k, v in merged.items()] 