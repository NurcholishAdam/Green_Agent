# src/reporting/report_generator.py

class ReportGenerator:
    def generate_full_report(self, results, scenario='production'):
        """Generate three-layer report"""
        reporter = LayeredReporter()
        
        reports = []
        for result in results:
            layer1 = reporter.generate_layer1(result)
            layer2 = reporter.generate_layer2(result, result['complexity'])
            layer3 = reporter.generate_layer3(result, scenario)
            
            reports.append({
                'agent_id': result['agent_id'],
                'layer1_raw': layer1.to_dict(),
                'layer2_normalized': layer2.to_dict(),
                'layer3_scenario': layer3.to_dict()
            })
        
        return reports

# Executive summary (Layer 3 focus)
executive_summary = report_gen.generate_executive_summary(full_report)

# Technical report (all layers)
technical_report = report_gen.generate_technical_report(full_report)

# Research report (Layer 1 + Layer 2)
research_report = report_gen.generate_research_report(full_report)
