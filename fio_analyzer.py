#!/usr/bin/env python3
"""
Анализ собранных результатов FIO тестов
"""

import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

class FIOAnalyzer:
    def __init__(self, results_dir: str = "fio_results"):
        self.results_dir = Path(results_dir)
        self.history_file = self.results_dir / "fio_history.json"
        
    def load_history(self):
        """Загрузка истории тестов"""
        if not self.history_file.exists():
            print("Файл истории не найден")
            return []
        
        with open(self.history_file, 'r') as f:
            return json.load(f)
    
    def create_timeseries_analysis(self):
        """Создание анализа временных рядов"""
        history = self.load_history()
        if not history:
            return
        
        # Преобразование в DataFrame
        data = []
        for result in history:
            timestamp = datetime.fromisoformat(result['timestamp'])
            test_name = result['test_name']
            
            for job_name, job_data in result.items():
                if job_name in ['timestamp', 'test_name', 'global_options']:
                    continue
                
                data.append({
                    'timestamp': timestamp,
                    'test_name': test_name,
                    'job_name': job_name,
                    'read_iops': job_data['read']['iops'],
                    'write_iops': job_data['write']['iops'],
                    'read_latency_p95_us': job_data['read']['percentiles'].get('95.000000', 0) / 1000,
                    'write_latency_p95_us': job_data['write']['percentiles'].get('95.000000', 0) / 1000,
                    'read_bw_kbps': job_data['read']['bw_kbps'],
                    'write_bw_kbps': job_data['write']['bw_kbps']
                })
        
        df = pd.DataFrame(data)
        
        # Сохранение в CSV
        csv_file = self.results_dir / "fio_metrics_timeseries.csv"
        df.to_csv(csv_file, index=False)
        print(f"Метрики сохранены в: {csv_file}")
        
        # Создание графиков
        self.create_plots(df)
        
        return df
    
    def create_plots(self, df):
        """Создание графиков производительности"""
        sns.set_style("whitegrid")
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # IOPS по времени
        for test_name in df['test_name'].unique():
            test_data = df[df['test_name'] == test_name]
            axes[0,0].plot(test_data['timestamp'], test_data['read_iops'], 
                          label=f'{test_name} Read', marker='o')
            axes[0,0].plot(test_data['timestamp'], test_data['write_iops'], 
                          label=f'{test_name} Write', marker='s')
        
        axes[0,0].set_title('IOPS Over Time')
        axes[0,0].set_ylabel('IOPS')
        axes[0,0].legend()
        axes[0,0].tick_params(axis='x', rotation=45)
        
        # Latency по времени
        for test_name in df['test_name'].unique():
            test_data = df[df['test_name'] == test_name]
            axes[0,1].plot(test_data['timestamp'], test_data['read_latency_p95_us'], 
                          label=f'{test_name} Read P95', marker='o')
            axes[0,1].plot(test_data['timestamp'], test_data['write_latency_p95_us'], 
                          label=f'{test_name} Write P95', marker='s')
        
        axes[0,1].set_title('P95 Latency Over Time (μs)')
        axes[0,1].set_ylabel('Latency (μs)')
        axes[0,1].legend()
        axes[0,1].tick_params(axis='x', rotation=45)
        
        # Bandwidth по времени
        for test_name in df['test_name'].unique():
            test_data = df[df['test_name'] == test_name]
            axes[1,0].plot(test_data['timestamp'], test_data['read_bw_kbps'] / 1024, 
                          label=f'{test_name} Read', marker='o')
            axes[1,0].plot(test_data['timestamp'], test_data['write_bw_kbps'] / 1024, 
                          label=f'{test_name} Write', marker='s')
        
        axes[1,0].set_title('Bandwidth Over Time (MB/s)')
        axes[1,0].set_ylabel('Bandwidth (MB/s)')
        axes[1,0].legend()
        axes[1,0].tick_params(axis='x', rotation=45)
        
        # Сводная статистика
        summary_stats = df.groupby('test_name').agg({
            'read_iops': ['mean', 'std', 'min', 'max'],
            'write_iops': ['mean', 'std', 'min', 'max'],
            'read_latency_p95_us': ['mean', 'std', 'min', 'max'],
            'write_latency_p95_us': ['mean', 'std', 'min', 'max']
        }).round(2)
        
        # Сохранение графиков
        plt.tight_layout()
        plot_file = self.results_dir / "performance_plots.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        print(f"Графики сохранены в: {plot_file}")
        
        # Сохранение статистики
        stats_file = self.results_dir / "performance_statistics.json"
        summary_stats.to_json(stats_file, indent=2)
        print(f"Статистика сохранена в: {stats_file}")

if __name__ == "__main__":
    analyzer = FIOAnalyzer()
    analyzer.create_timeseries_analysis()