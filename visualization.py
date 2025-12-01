# visualization (plots) script

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle
import logging
import os
import textwrap

PLOTS_DIR = "plots"
os.makedirs(PLOTS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename='visualization.log'
)

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['legend.fontsize'] = 10

class InsightGenerator:
  
    """generating visualizations and key insights from our analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.insights = []
    
    def load_data(self):
      
        """loading all analysis results"""
      
        try:
            self.cuisine_df = pd.read_csv(os.path.join('analysis_tables', 'cuisine_analysis.csv'))
            self.borough_df = pd.read_csv(os.path.join('analysis_tables', 'borough_analysis.csv'))
            self.temporal_df = pd.read_csv(os.path.join('analysis_tables', 'temporal_trends.csv'))
            self.anomalies_df = pd.read_csv(os.path.join('analysis_tables', 'anomalies_detected.csv'))
            self.property_df = pd.read_csv(os.path.join('analysis_tables', 'property_value_analysis.csv'))
            self.violations_df = pd.read_csv(os.path.join('analysis_tables', 'violation_frequency.csv'))
            self.analysis_df = pd.read_csv(os.path.join('analysis_tables', 'analysis_data.csv'))
            print("All analysis data loaded")
            return True
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            print(f"Error loading data: {e}")
            return False
    
    def plot_cuisine_performance(self):
      
        """plot 1 - top 15 cuisines by average score (to find which cuisines have better/worse compliance)"""
      
        try:
            print("Creating cuisine performance plot")
            
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # sorting by avg_score and get top 15
            top_cuisines = self.cuisine_df.nlargest(15, 'avg_score')
            
            # creating color gradient based on score
            colors = plt.cm.RdYlGn(
                (top_cuisines['avg_score'] - top_cuisines['avg_score'].min()) / 
                (top_cuisines['avg_score'].max() - top_cuisines['avg_score'].min())
            )
            
            bars = ax.barh(range(len(top_cuisines)), top_cuisines['avg_score'], color=colors)
            ax.set_yticks(range(len(top_cuisines)))
            ax.set_yticklabels(top_cuisines['cuisine'].str.title(), fontsize=11)
            ax.set_xlabel('Average Inspection Score', fontsize=12, fontweight='bold')
            ax.set_title('NYC Cuisine Performance: Average Inspection Scores\n(Lower is Better)', 
                        fontsize=14, fontweight='bold', pad=20)
            
            # adding score labels on bars
            for i, (idx, row) in enumerate(top_cuisines.iterrows()):
                ax.text(row['avg_score'] + 0.5, i, f"{row['avg_score']:.1f}", 
                       va='center', fontsize=10)
            
            ax.set_xlim(0, top_cuisines['avg_score'].max() + 5)
            ax.grid(axis='x', alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(os.path.join(PLOTS_DIR, 'plot_1_cuisine_performance.png'), dpi=300, bbox_inches='tight')
            print("Saved: plot_1_cuisine_performance.png")
            plt.close()
            
        except Exception as e:
            self.logger.error(f"Error creating cuisine plot: {e}")
            print(f"Error: {e}")
    
    def plot_borough_comparison(self):
      
        """plot 2 - borough-level safety comparison (to find boroughs with highest violation rates)"""
      
        try:
            print("Creating borough comparison plot")
            
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            
            # average score by borough plot
            ax = axes[0, 0]
            borough_sorted = self.borough_df.sort_values('avg_score')
            colors = plt.cm.RdYlGn(
                (borough_sorted['avg_score'] - borough_sorted['avg_score'].min()) / 
                (borough_sorted['avg_score'].max() - borough_sorted['avg_score'].min())
            )
            ax.barh(borough_sorted['borough'].str.title(), borough_sorted['avg_score'], color=colors)
            ax.set_xlabel('Average Score (Lower is Better)', fontweight='bold')
            ax.set_title('Average Inspection Score by Borough', fontweight='bold', fontsize=12)
            ax.grid(axis='x', alpha=0.3)
            
            # critical violation rate plot
            ax = axes[0, 1]
            critical_sorted = self.borough_df.sort_values('critical_violation_rate', ascending=False)
            ax.bar(critical_sorted['borough'].str.title(), 
                   critical_sorted['critical_violation_rate'] * 100, 
                   color='#d62728', alpha=0.7)
            ax.set_ylabel('Critical Violation Rate (%)', fontweight='bold')
            ax.set_title('Critical Violations by Borough', fontweight='bold', fontsize=12)
            ax.grid(axis='y', alpha=0.3)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            # grade distribution plot
            ax = axes[1, 0]
            grade_data = self.borough_df[['borough', 'grade_a_pct', 'grade_b_pct', 'grade_c_pct']].copy()
            grade_data['borough'] = grade_data['borough'].str.title()
            grade_data.set_index('borough')[['grade_a_pct', 'grade_b_pct', 'grade_c_pct']].plot(
                kind='bar', stacked=True, ax=ax, 
                color=['#2ecc71', '#f39c12', '#e74c3c']
            )
            ax.set_ylabel('Percentage of Inspections', fontweight='bold')
            ax.set_title('Grade Distribution by Borough', fontweight='bold', fontsize=12)
            ax.legend(['Grade A', 'Grade B', 'Grade C'], loc='upper right')
            ax.set_ylim(0, 1)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            # sample size plot
            ax = axes[1, 1]
            restaurants_sorted = self.borough_df.sort_values('num_restaurants', ascending=False)
            ax.bar(restaurants_sorted['borough'].str.title(), restaurants_sorted['num_restaurants'], 
                  color='#3498db', alpha=0.7)
            ax.set_ylabel('Number of Restaurants', fontweight='bold')
            ax.set_title('Number of Restaurants by Borough', fontweight='bold', fontsize=12)
            ax.grid(axis='y', alpha=0.3)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            plt.suptitle('NYC Borough Safety Comparison', fontsize=16, fontweight='bold', y=0.995)
            plt.tight_layout()
            plt.savefig(os.path.join(PLOTS_DIR, 'plot_2_borough_comparison.png'), dpi=300, bbox_inches='tight')
            print("Saved: plot_2_borough_comparison.png")
            plt.close()
            
        except Exception as e:
            self.logger.error(f"Error creating borough plot: {e}")
            print(f"Error: {e}")
    
    def plot_temporal_trends(self):
      
        """plot 3 - temporal trends to show inspection outcomes over time (to find improving/declining trends in NYC food safety)"""
      
        try:
            print("Creating temporal trends plot")
            
            # aggregating by year to get clearer trend
            temporal_yearly = self.temporal_df.groupby('inspection_year').agg({
                'avg_score': 'mean',
                'critical_rate': 'mean',
                'num_inspections': 'sum'
            }).reset_index()
            
            fig, axes = plt.subplots(2, 1, figsize=(14, 10))
            
            # average score trend plot
            ax = axes[0]
            ax.plot(temporal_yearly['inspection_year'], temporal_yearly['avg_score'], 
                   marker='o', linewidth=2.5, markersize=8, color='#e74c3c', label='Avg Score')
            ax.fill_between(temporal_yearly['inspection_year'], temporal_yearly['avg_score'], 
                            alpha=0.3, color='#e74c3c')
            ax.set_ylabel('Average Inspection Score', fontweight='bold')
            ax.set_title('Food Safety Trend: Inspection Scores Over Time\n(Lower scores = Better compliance)', 
                        fontweight='bold', fontsize=13)
            ax.grid(True, alpha=0.3)
            
            # adding annotation for trend
            if temporal_yearly['avg_score'].iloc[-1] < temporal_yearly['avg_score'].iloc[0]:
                trend_text = "✓ IMPROVING TREND"
                color = '#2ecc71'
            else:
                trend_text = "✗ DECLINING TREND"
                color = '#e74c3c'
            ax.text(0.02, 0.95, trend_text, transform=ax.transAxes, 
                   fontsize=12, fontweight='bold', color=color,
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                   verticalalignment='top')
            
            # critical violations trend plot
            ax = axes[1]
            ax.plot(temporal_yearly['inspection_year'], temporal_yearly['critical_rate'] * 100, 
                   marker='s', linewidth=2.5, markersize=8, color='#9b59b6', label='Critical Violation Rate')
            ax.fill_between(temporal_yearly['inspection_year'], temporal_yearly['critical_rate'] * 100, 
                            alpha=0.3, color='#9b59b6')
            ax.set_xlabel('Year', fontweight='bold')
            ax.set_ylabel('Critical Violation Rate (%)', fontweight='bold')
            ax.set_title('Critical Violations Over Time', fontweight='bold', fontsize=13)
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(os.path.join(PLOTS_DIR, 'plot_3_temporal_trends.png'), dpi=300, bbox_inches='tight')
            print("Saved: plot_3_temporal_trends.png")
            plt.close()
            
        except Exception as e:
            self.logger.error(f"Error creating temporal plot: {e}")
            print(f"Error: {e}")
    
    def plot_property_value_impact(self):
      
        """plot 4 - property value impact on inspection outcomes (e.g. higher property value → better compliance)"""
      
        try:
            print("Creating property value impact plot")
            
            # ordering property values
            value_order = ['$0-100K', '$100K-500K', '$500K-1M', '$1M-5M', '$5M+', 'Unknown']
            prop_ordered = self.property_df.copy()
            prop_ordered['property_value_range'] = pd.Categorical(
                prop_ordered['property_value_range'], 
                categories=value_order, 
                ordered=True
            )
            prop_ordered = prop_ordered.sort_values('property_value_range')
            
            fig, axes = plt.subplots(1, 2, figsize=(16, 6))
            
            # average score vs property value plot
            ax = axes[0]
            colors_grad = plt.cm.RdYlGn(
                (prop_ordered['avg_score'] - prop_ordered['avg_score'].min()) / 
                (prop_ordered['avg_score'].max() - prop_ordered['avg_score'].min())
            )
            bars = ax.bar(range(len(prop_ordered)), prop_ordered['avg_score'], color=colors_grad, alpha=0.8)
            ax.set_xticks(range(len(prop_ordered)))
            ax.set_xticklabels(prop_ordered['property_value_range'], rotation=45, ha='right')
            ax.set_ylabel('Average Inspection Score (Lower = Better)', fontweight='bold')
            ax.set_title('Inspection Performance by Property Value\n(KEY INSIGHT: Expensive properties have better compliance)', 
                        fontweight='bold', fontsize=12)
            ax.grid(axis='y', alpha=0.3)
            
            # adding values on bars
            for bar, val in zip(bars, prop_ordered['avg_score']):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{val:.1f}',
                       ha='center', va='bottom', fontweight='bold', fontsize=10)
            
            # citical violation rate vs property value plot
            ax = axes[1]
            colors_crit = ['#e74c3c' if x > prop_ordered['critical_rate'].median() else '#2ecc71' 
                          for x in prop_ordered['critical_rate']]
            bars = ax.bar(range(len(prop_ordered)), prop_ordered['critical_rate'] * 100, 
                         color=colors_crit, alpha=0.8)
            ax.set_xticks(range(len(prop_ordered)))
            ax.set_xticklabels(prop_ordered['property_value_range'], rotation=45, ha='right')
            ax.set_ylabel('Critical Violation Rate (%)', fontweight='bold')
            ax.set_title('Critical Violations by Property Value', fontweight='bold', fontsize=12)
            ax.grid(axis='y', alpha=0.3)
            
            # adding values on bars
            for bar, val in zip(bars, prop_ordered['critical_rate'] * 100):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{val:.1f}%',
                       ha='center', va='bottom', fontweight='bold', fontsize=9)
            
            plt.suptitle('Property Value Impact on Restaurant Safety', 
                        fontsize=14, fontweight='bold')
            plt.tight_layout()
            plt.savefig(os.path.join(PLOTS_DIR, 'plot_4_property_value_impact.png'), dpi=300, bbox_inches='tight')
            print("Saved: plot_4_property_value_impact.png")
            plt.close()
            
        except Exception as e:
            self.logger.error(f"Error creating property plot: {e}")
            print(f"Error: {e}")
    
    def plot_top_violations(self):
      
        """plot 5 - top 10 most common violations (to find systemic food safety issues in NYC)"""
      
        try:
            print("Creating top violations plot")
            
            top_violations = self.violations_df.head(10).copy()
            
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # creating horizontal bar chart
            bars = ax.barh(range(len(top_violations)), top_violations['frequency'], 
                          color=plt.cm.Spectral(np.linspace(0, 1, len(top_violations))))
            
            wrapped_labels = [
                f"{i+1}. " + "\n   ".join(textwrap.wrap(v, width=50))
                for i, v in enumerate(top_violations['violation_category'])
            ]

            ax.set_yticks(range(len(top_violations)))
            ax.set_yticklabels(wrapped_labels, fontsize=10)
            
            ax.set_xlabel('Number of Violations Cited', fontweight='bold')
            ax.set_title('Top 10 Most Common Violations in NYC Restaurants\n(Systemic Food Safety Issues)', 
                        fontweight='bold', fontsize=13, pad=20)
            
            # adding frequency labels
            for i, (idx, row) in enumerate(top_violations.iterrows()):
                ax.text(row['frequency'] + 100, i, 
                       f"{int(row['frequency']):,} violations\n({int(row['restaurants_affected'])} restaurants)", 
                       va='center', fontsize=9, fontweight='bold')
            
            ax.grid(axis='x', alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(os.path.join(PLOTS_DIR, 'plot_5_top_violations.png'), dpi=300, bbox_inches='tight')
            print("Saved: plot_5_top_violations.png")
            plt.close()
            
        except Exception as e:
            self.logger.error(f"Error creating violations plot: {e}")
            print(f"Error: {e}")
    
    def generate_key_findings(self):
      
        """generating key findings"""
      
        try:
            print("\n" + "="*70)
            print("KEY FINDINGS")
            print("="*70 + "\n")
            
            findings = []
            
            # property values (finding 1)
            avg_expensive = self.property_df[
                self.property_df['property_value_range'] == '$5M+'
            ]['avg_score'].values
            avg_cheap = self.property_df[
                self.property_df['property_value_range'] == '$0-100K'
            ]['avg_score'].values
            
            if len(avg_expensive) > 0 and len(avg_cheap) > 0:
                diff = avg_cheap[0] - avg_expensive[0]
              
                finding = f"""
                FINDING 1: Property Value Correlation --
                High-value properties ($5M+) have {diff:.1f} BETTER average scores 
                than low-value properties ($0-100K). This suggests that upscale 
                establishments invest more in food safety compliance. This implies
                property value is a proxy for restaurant quality/compliance.
                """
              
                print(finding)
                findings.append(finding)
            
            # borough disparities (finding 2)
            worst_borough = self.borough_df.loc[self.borough_df['critical_violation_rate'].idxmax()]
            best_borough = self.borough_df.loc[self.borough_df['critical_violation_rate'].idxmin()]
            
            critical_diff = (worst_borough['critical_violation_rate'] - 
                           best_borough['critical_violation_rate']) * 100
            
            finding = f"""
            FINDING 2: Significant Borough Disparities --
            {worst_borough['borough'].upper()} has {critical_diff:.1f}% MORE critical violations 
            than {best_borough['borough'].upper()}. This reflects socioeconomic and 
            regulatory enforcement disparities across NYC. This implies
            public health policy should address unequal enforcement.
            """
          
            print(finding)
            findings.append(finding)
            
            # cuisine performance (finding 3)
            safest_cuisine = self.cuisine_df.loc[self.cuisine_df['avg_score'].idxmin()]
            riskiest_cuisine = self.cuisine_df.loc[self.cuisine_df['avg_score'].idxmax()]
            
            cuisine_diff = riskiest_cuisine['avg_score'] - safest_cuisine['avg_score']
            
            finding = f"""
            FINDING 3: Cuisine-Specific Safety Patterns --
            {riskiest_cuisine['cuisine'].upper()} restaurants score {cuisine_diff:.1f} points WORSE 
            than {safest_cuisine['cuisine'].upper()} restaurants. This suggests 
            cuisine type correlates with operational complexity/food safety. This
            implies inspection protocols should be tailored by cuisine type.
            """
          
            print(finding)
            findings.append(finding)
            
            # temporal trends (finding 4)
            temporal_yearly = self.temporal_df.groupby('inspection_year').agg({
                'avg_score': 'mean'
            }).reset_index()
            
            if len(temporal_yearly) > 1:
                first_year_score = temporal_yearly.iloc[0]['avg_score']
                last_year_score = temporal_yearly.iloc[-1]['avg_score']
                trend = 'IMPROVING' if last_year_score < first_year_score else 'WORSENING'
                change = abs(last_year_score - first_year_score)
                
                finding = f"""
                FINDING 4: Long-term Food Safety Trend --
                NYC restaurant safety is {trend} over time, with a {change:.1f} point 
                change since {int(temporal_yearly.iloc[0]['inspection_year'])}. This
                implies food safety initiatives are {'showing positive results' if trend == 'IMPROVING' else 'need intervention'}.
                """
              
                print(finding)
                findings.append(finding)
            
            # anomalies (finding 5)
            high_volatility = self.anomalies_df[
                self.anomalies_df['anomaly_type'] == 'High Score Volatility'
            ]
            
            if len(high_volatility) > 0:
                avg_volatility_count = len(high_volatility)
              
                finding = f"""
                FINDING 5: Restaurant-Level Anomalies Detected --
                {avg_volatility_count} restaurants show highly volatile inspection scores, 
                suggesting inconsistent food safety practices or uneven inspector evaluation.
                This implies these restaurants warrant targeted inspection focus and intervention.
                """
              
                print(finding)
                findings.append(finding)
            
            # most common violations (finding 6)
            top_violation = self.violations_df.iloc[0]
            
            finding = f"""
            FINDING 6: Systemic Food Safety Issues --
            The most common violation ({top_violation['violation_category'][:60]}...) 
            appears in {int(top_violation['frequency']):,} inspections across
            {int(top_violation['restaurants_affected'])} restaurants. This systemic issue 
            suggests need for city-wide training/enforcement. This implies targeted
            public health education could address widespread compliance gaps.
            """
          
            print(finding)
            findings.append(finding)
            
            print("="*70 + "\n")
            
            return findings
            
        except Exception as e:
            self.logger.error(f"Error generating findings: {e}")
            print(f"Error generating findings: {e}")
            return []
    
    def create_all_visualizations(self):
      
        """running all visualizations"""
      
        try:
            print("\n" + "="*70)
            print("VISUALIZATION & INSIGHT GENERATION")
            print("="*70 + "\n")
            
            if not self.load_data():
                return False
            
            print("\nGenerating plots")
            self.plot_cuisine_performance()
            self.plot_borough_comparison()
            self.plot_temporal_trends()
            self.plot_property_value_impact()
            self.plot_top_violations()
            
            findings = self.generate_key_findings()
            
            print("All visualizations complete")
            print("Files generated:")
            print("- plot_1_cuisine_performance.png")
            print("- plot_2_borough_comparison.png")
            print("- plot_3_temporal_trends.png")
            print("- plot_4_property_value_impact.png")
            print("- plot_5_top_violations.png")
            print("="*70 + "\n")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in visualization: {e}")
            print(f"Error: {e}")
            return False


if __name__ == "__main__":
    generator = InsightGenerator()
    generator.create_all_visualizations()
