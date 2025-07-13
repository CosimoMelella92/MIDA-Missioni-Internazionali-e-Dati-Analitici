"""
Advanced Report Generator for PDF Extraction
Generates comprehensive reports with analytics and visualizations
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Any
import numpy as np

class AdvancedReportGenerator:
    """Generates comprehensive extraction reports with analytics"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.report_data = {}
        
    def generate_comprehensive_report(self, extraction_results: List[Dict], 
                                   output_dir: str = "reports/pdf_extraction") -> Dict[str, Any]:
        """
        Generate comprehensive extraction report
        
        Args:
            extraction_results: List of extraction results
            output_dir: Directory to save reports
            
        Returns:
            Dictionary with report data and file paths
        """
        self.logger.info("Generating comprehensive extraction report...")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Analyze extraction results
        analysis = self._analyze_extraction_results(extraction_results)
        
        # Generate different report sections
        reports = {
            'summary': self._generate_summary_report(analysis, output_path),
            'detailed': self._generate_detailed_report(extraction_results, output_path),
            'analytics': self._generate_analytics_report(analysis, output_path),
            'visualizations': self._generate_visualization_report(analysis, output_path),
            'quality': self._generate_quality_report(extraction_results, output_path)
        }
        
        # Generate main HTML report
        main_report = self._generate_main_html_report(reports, analysis, output_path)
        
        self.report_data = {
            'analysis': analysis,
            'reports': reports,
            'main_report': main_report,
            'timestamp': datetime.now().isoformat()
        }
        
        return self.report_data
    
    def _analyze_extraction_results(self, results: List[Dict]) -> Dict[str, Any]:
        """Analyze extraction results for insights"""
        analysis = {
            'total_files': len(results),
            'file_types': {},
            'missions_found': [],
            'countries_mentioned': [],
            'personnel_totals': [],
            'cost_totals': [],
            'confidence_scores': [],
            'extraction_errors': [],
            'quality_metrics': {}
        }
        
        for result in results:
            # File type analysis
            file_type = result.get('type', 'unknown')
            analysis['file_types'][file_type] = analysis['file_types'].get(file_type, 0) + 1
            
            # Extract structured data
            structured = result.get('structured_data', {})
            
            # Missions analysis
            missions = structured.get('missions', [])
            for mission in missions:
                analysis['missions_found'].append({
                    'name': mission.get('name', ''),
                    'confidence': mission.get('confidence', 0),
                    'file': result.get('file', ''),
                    'type': file_type
                })
            
            # Countries analysis
            countries = structured.get('countries', [])
            for country in countries:
                analysis['countries_mentioned'].append({
                    'name': country.get('name', ''),
                    'confidence': country.get('confidence', 0),
                    'file': result.get('file', '')
                })
            
            # Personnel analysis
            personnel = structured.get('personnel', [])
            total_personnel = sum([p.get('number', 0) for p in personnel])
            if total_personnel > 0:
                analysis['personnel_totals'].append({
                    'file': result.get('file', ''),
                    'total': total_personnel,
                    'details': personnel
                })
            
            # Costs analysis
            costs = structured.get('costs', [])
            total_cost = sum([c.get('amount', 0) for c in costs])
            if total_cost > 0:
                analysis['cost_totals'].append({
                    'file': result.get('file', ''),
                    'total': total_cost,
                    'details': costs
                })
            
            # Confidence analysis
            overall_confidence = structured.get('confidence', 0)
            analysis['confidence_scores'].append({
                'file': result.get('file', ''),
                'confidence': overall_confidence,
                'type': file_type
            })
            
            # Error analysis
            if 'error' in result:
                analysis['extraction_errors'].append({
                    'file': result.get('file', ''),
                    'error': result['error'],
                    'type': file_type
                })
        
        # Calculate quality metrics
        analysis['quality_metrics'] = self._calculate_quality_metrics(analysis)
        
        return analysis
    
    def _calculate_quality_metrics(self, analysis: Dict) -> Dict[str, Any]:
        """Calculate quality metrics for extraction"""
        total_files = analysis['total_files']
        if total_files == 0:
            return {}
        
        # Success rate
        successful_files = total_files - len(analysis['extraction_errors'])
        success_rate = (successful_files / total_files) * 100
        
        # Average confidence
        confidences = [c['confidence'] for c in analysis['confidence_scores']]
        avg_confidence = np.mean(confidences) if confidences else 0
        
        # Data richness
        files_with_missions = len(set([m['file'] for m in analysis['missions_found']]))
        files_with_countries = len(set([c['file'] for c in analysis['countries_mentioned']]))
        files_with_personnel = len(analysis['personnel_totals'])
        files_with_costs = len(analysis['cost_totals'])
        
        return {
            'success_rate': success_rate,
            'average_confidence': avg_confidence,
            'files_with_missions': files_with_missions,
            'files_with_countries': files_with_countries,
            'files_with_personnel': files_with_personnel,
            'files_with_costs': files_with_costs,
            'data_richness_score': (files_with_missions + files_with_countries + 
                                  files_with_personnel + files_with_costs) / (total_files * 4) * 100
        }
    
    def _generate_summary_report(self, analysis: Dict, output_path: Path) -> str:
        """Generate summary report"""
        summary_data = {
            'total_files_processed': analysis['total_files'],
            'successful_extractions': analysis['total_files'] - len(analysis['extraction_errors']),
            'total_missions_found': len(analysis['missions_found']),
            'total_countries_mentioned': len(set([c['name'] for c in analysis['countries_mentioned']])),
            'total_personnel': sum([p['total'] for p in analysis['personnel_totals']]),
            'total_costs': sum([c['total'] for c in analysis['cost_totals']]),
            'average_confidence': analysis['quality_metrics'].get('average_confidence', 0),
            'success_rate': analysis['quality_metrics'].get('success_rate', 0)
        }
        
        # Create summary DataFrame
        df_summary = pd.DataFrame([summary_data])
        
        # Save summary
        summary_file = output_path / "extraction_summary.csv"
        df_summary.to_csv(summary_file, index=False)
        
        return str(summary_file)
    
    def _generate_detailed_report(self, results: List[Dict], output_path: Path) -> str:
        """Generate detailed extraction report"""
        detailed_data = []
        
        for result in results:
            structured = result.get('structured_data', {})
            
            detailed_data.append({
                'file_name': result.get('file', ''),
                'file_type': result.get('type', ''),
                'extraction_success': 'error' not in result,
                'missions_found': len(structured.get('missions', [])),
                'countries_found': len(structured.get('countries', [])),
                'personnel_numbers': len(structured.get('personnel', [])),
                'cost_entries': len(structured.get('costs', [])),
                'overall_confidence': structured.get('confidence', 0),
                'text_length': len(result.get('raw_data', {}).get('full_text', '')),
                'pages_processed': result.get('raw_data', {}).get('pages', 0)
            })
        
        df_detailed = pd.DataFrame(detailed_data)
        
        # Save detailed report
        detailed_file = output_path / "detailed_extraction_report.csv"
        df_detailed.to_csv(detailed_file, index=False)
        
        return str(detailed_file)
    
    def _generate_analytics_report(self, analysis: Dict, output_path: Path) -> str:
        """Generate analytics report with insights"""
        analytics_data = {
            'file_type_distribution': analysis['file_types'],
            'mission_analysis': self._analyze_missions(analysis['missions_found']),
            'country_analysis': self._analyze_countries(analysis['countries_mentioned']),
            'personnel_analysis': self._analyze_personnel(analysis['personnel_totals']),
            'cost_analysis': self._analyze_costs(analysis['cost_totals']),
            'confidence_distribution': self._analyze_confidence(analysis['confidence_scores']),
            'error_analysis': self._analyze_errors(analysis['extraction_errors'])
        }
        
        # Save analytics
        analytics_file = output_path / "extraction_analytics.json"
        with open(analytics_file, 'w', encoding='utf-8') as f:
            json.dump(analytics_data, f, indent=2, ensure_ascii=False)
        
        return str(analytics_file)
    
    def _generate_visualization_report(self, analysis: Dict, output_path: Path) -> str:
        """Generate visualization report with charts"""
        charts = {}
        
        # File type distribution
        if analysis['file_types']:
            fig_types = px.pie(
                values=list(analysis['file_types'].values()),
                names=list(analysis['file_types'].keys()),
                title="Distribuzione Tipi di File"
            )
            charts['file_types'] = fig_types
        
        # Mission confidence distribution
        if analysis['missions_found']:
            mission_confidences = [m['confidence'] for m in analysis['missions_found']]
            fig_missions = px.histogram(
                x=mission_confidences,
                title="Distribuzione Confidenza Missioni",
                labels={'x': 'Confidenza', 'y': 'Frequenza'}
            )
            charts['mission_confidence'] = fig_missions
        
        # Personnel distribution
        if analysis['personnel_totals']:
            personnel_values = [p['total'] for p in analysis['personnel_totals']]
            fig_personnel = px.bar(
                x=[p['file'] for p in analysis['personnel_totals']],
                y=personnel_values,
                title="Personale per File",
                labels={'x': 'File', 'y': 'Personale'}
            )
            charts['personnel'] = fig_personnel
        
        # Save charts
        charts_file = output_path / "extraction_charts.html"
        with open(charts_file, 'w', encoding='utf-8') as f:
            f.write("<html><head><title>Extraction Charts</title></head><body>")
            for name, fig in charts.items():
                f.write(f"<h2>{name}</h2>")
                f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write("</body></html>")
        
        return str(charts_file)
    
    def _generate_quality_report(self, results: List[Dict], output_path: Path) -> str:
        """Generate quality assessment report"""
        quality_data = []
        
        for result in results:
            structured = result.get('structured_data', {})
            
            # Calculate quality score
            quality_score = 0
            factors = []
            
            # Factor 1: Text extraction success
            if 'raw_data' in result and result['raw_data'].get('full_text'):
                quality_score += 25
                factors.append("Testo estratto")
            
            # Factor 2: Missions found
            if structured.get('missions'):
                quality_score += 25
                factors.append("Missioni trovate")
            
            # Factor 3: Countries found
            if structured.get('countries'):
                quality_score += 25
                factors.append("Paesi identificati")
            
            # Factor 4: Numerical data
            if structured.get('personnel') or structured.get('costs'):
                quality_score += 25
                factors.append("Dati numerici")
            
            quality_data.append({
                'file': result.get('file', ''),
                'type': result.get('type', ''),
                'quality_score': quality_score,
                'confidence': structured.get('confidence', 0),
                'factors': ', '.join(factors),
                'has_error': 'error' in result
            })
        
        df_quality = pd.DataFrame(quality_data)
        
        # Save quality report
        quality_file = output_path / "quality_assessment.csv"
        df_quality.to_csv(quality_file, index=False)
        
        return str(quality_file)
    
    def _generate_main_html_report(self, reports: Dict, analysis: Dict, output_path: Path) -> str:
        """Generate main HTML report"""
        html_content = f"""
        <!DOCTYPE html>
        <html lang="it">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Report Estrazione PDF - MIDA Project</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
            <style>
                .metric-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }}
                .quality-high {{ color: #28a745; }}
                .quality-medium {{ color: #ffc107; }}
                .quality-low {{ color: #dc3545; }}
            </style>
        </head>
        <body class="bg-light">
            <div class="container-fluid py-4">
                <div class="row">
                    <div class="col-12">
                        <h1 class="text-center mb-4">
                            📊 Report Estrazione PDF - MIDA Project
                        </h1>
                        <p class="text-center text-muted">
                            Generato il {datetime.now().strftime('%d/%m/%Y alle %H:%M')}
                        </p>
                    </div>
                </div>
                
                <!-- Summary Metrics -->
                <div class="row mb-4">
                    <div class="col-md-3">
                        <div class="card metric-card">
                            <div class="card-body text-center">
                                <h4>{analysis['total_files']}</h4>
                                <p>File Processati</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card metric-card">
                            <div class="card-body text-center">
                                <h4>{len(analysis['missions_found'])}</h4>
                                <p>Missioni Trovate</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card metric-card">
                            <div class="card-body text-center">
                                <h4>{analysis['quality_metrics'].get('success_rate', 0):.1f}%</h4>
                                <p>Tasso di Successo</p>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card metric-card">
                            <div class="card-body text-center">
                                <h4>{analysis['quality_metrics'].get('average_confidence', 0):.1f}%</h4>
                                <p>Confidenza Media</p>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Quality Assessment -->
                <div class="row mb-4">
                    <div class="col-12">
                        <div class="card">
                            <div class="card-header">
                                <h5>📈 Valutazione Qualità Estrazione</h5>
                            </div>
                            <div class="card-body">
                                <div class="row">
                                    <div class="col-md-6">
                                        <h6>Metriche di Qualità:</h6>
                                        <ul>
                                            <li>File con missioni: {analysis['quality_metrics'].get('files_with_missions', 0)}</li>
                                            <li>File con paesi: {analysis['quality_metrics'].get('files_with_countries', 0)}</li>
                                            <li>File con personale: {analysis['quality_metrics'].get('files_with_personnel', 0)}</li>
                                            <li>File con costi: {analysis['quality_metrics'].get('files_with_costs', 0)}</li>
                                        </ul>
                                    </div>
                                    <div class="col-md-6">
                                        <h6>Punteggio Ricchezza Dati:</h6>
                                        <div class="progress mb-3">
                                            <div class="progress-bar" style="width: {analysis['quality_metrics'].get('data_richness_score', 0)}%">
                                                {analysis['quality_metrics'].get('data_richness_score', 0):.1f}%
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- File Details -->
                <div class="row mb-4">
                    <div class="col-12">
                        <div class="card">
                            <div class="card-header">
                                <h5>📁 Dettagli File Processati</h5>
                            </div>
                            <div class="card-body">
                                <div class="table-responsive">
                                    <table class="table table-striped">
                                        <thead>
                                            <tr>
                                                <th>File</th>
                                                <th>Tipo</th>
                                                <th>Missioni</th>
                                                <th>Paesi</th>
                                                <th>Personale</th>
                                                <th>Costi</th>
                                                <th>Confidenza</th>
                                            </tr>
                                        </thead>
                                        <tbody>
        """
        
        # Add file details
        for result in results:
            structured = result.get('structured_data', {})
            missions_count = len(structured.get('missions', []))
            countries_count = len(structured.get('countries', []))
            personnel_count = len(structured.get('personnel', []))
            costs_count = len(structured.get('costs', []))
            confidence = structured.get('confidence', 0)
            
            confidence_class = "quality-high" if confidence > 0.7 else "quality-medium" if confidence > 0.4 else "quality-low"
            
            html_content += f"""
                                            <tr>
                                                <td>{result.get('file', '')}</td>
                                                <td>{result.get('type', '')}</td>
                                                <td>{missions_count}</td>
                                                <td>{countries_count}</td>
                                                <td>{personnel_count}</td>
                                                <td>{costs_count}</td>
                                                <td class="{confidence_class}">{confidence:.1%}</td>
                                            </tr>
            """
        
        html_content += """
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Download Links -->
                <div class="row">
                    <div class="col-12">
                        <div class="card">
                            <div class="card-header">
                                <h5>📥 Download Report</h5>
                            </div>
                            <div class="card-body">
                                <div class="row">
                                    <div class="col-md-3">
                                        <a href="extraction_summary.csv" class="btn btn-primary w-100">
                                            📊 Summary CSV
                                        </a>
                                    </div>
                                    <div class="col-md-3">
                                        <a href="detailed_extraction_report.csv" class="btn btn-info w-100">
                                            📋 Detailed Report
                                        </a>
                                    </div>
                                    <div class="col-md-3">
                                        <a href="quality_assessment.csv" class="btn btn-warning w-100">
                                            📈 Quality Assessment
                                        </a>
                                    </div>
                                    <div class="col-md-3">
                                        <a href="extraction_charts.html" class="btn btn-success w-100">
                                            📊 Charts
                                        </a>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Save main report
        main_report_file = output_path / "extraction_report.html"
        with open(main_report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return str(main_report_file)
    
    def _analyze_missions(self, missions: List[Dict]) -> Dict:
        """Analyze mission data"""
        if not missions:
            return {}
        
        mission_names = [m['name'] for m in missions]
        mission_confidences = [m['confidence'] for m in missions]
        
        return {
            'total_missions': len(missions),
            'unique_missions': len(set(mission_names)),
            'avg_confidence': np.mean(mission_confidences),
            'top_missions': pd.Series(mission_names).value_counts().head(10).to_dict()
        }
    
    def _analyze_countries(self, countries: List[Dict]) -> Dict:
        """Analyze country data"""
        if not countries:
            return {}
        
        country_names = [c['name'] for c in countries]
        country_confidences = [c['confidence'] for c in countries]
        
        return {
            'total_countries': len(countries),
            'unique_countries': len(set(country_names)),
            'avg_confidence': np.mean(country_confidences),
            'top_countries': pd.Series(country_names).value_counts().head(10).to_dict()
        }
    
    def _analyze_personnel(self, personnel: List[Dict]) -> Dict:
        """Analyze personnel data"""
        if not personnel:
            return {}
        
        personnel_totals = [p['total'] for p in personnel]
        
        return {
            'total_personnel': sum(personnel_totals),
            'avg_personnel': np.mean(personnel_totals),
            'max_personnel': max(personnel_totals),
            'min_personnel': min(personnel_totals)
        }
    
    def _analyze_costs(self, costs: List[Dict]) -> Dict:
        """Analyze cost data"""
        if not costs:
            return {}
        
        cost_totals = [c['total'] for c in costs]
        
        return {
            'total_costs': sum(cost_totals),
            'avg_costs': np.mean(cost_totals),
            'max_costs': max(cost_totals),
            'min_costs': min(cost_totals)
        }
    
    def _analyze_confidence(self, confidences: List[Dict]) -> Dict:
        """Analyze confidence scores"""
        if not confidences:
            return {}
        
        confidence_scores = [c['confidence'] for c in confidences]
        
        return {
            'avg_confidence': np.mean(confidence_scores),
            'max_confidence': max(confidence_scores),
            'min_confidence': min(confidence_scores),
            'high_confidence_files': len([c for c in confidence_scores if c > 0.7]),
            'medium_confidence_files': len([c for c in confidence_scores if 0.4 <= c <= 0.7]),
            'low_confidence_files': len([c for c in confidence_scores if c < 0.4])
        }
    
    def _analyze_errors(self, errors: List[Dict]) -> Dict:
        """Analyze extraction errors"""
        if not errors:
            return {}
        
        error_types = [e.get('error', 'Unknown') for e in errors]
        file_types = [e.get('type', 'Unknown') for e in errors]
        
        return {
            'total_errors': len(errors),
            'error_types': pd.Series(error_types).value_counts().to_dict(),
            'file_types_with_errors': pd.Series(file_types).value_counts().to_dict()
        } 