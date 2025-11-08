#!/usr/bin/env python3
"""
FIO Monitor - периодический запуск тестов производительности хранилища
с обработкой результатов в JSON формате
"""

import json
import subprocess
import time
import logging
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional

class FIOMonitor:
    def __init__(self, config_file: str = None, output_dir: str = "fio_results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Настройка логирования
        self.setup_logging()
        
        # Конфигурация тестов
        self.configs = self.load_configs(config_file)
        
        # История результатов
        self.history = []
        
    def setup_logging(self):
        """Настройка системы логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'fio_monitor.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_configs(self, config_file: str = None) -> List[Dict]:
        """Загрузка конфигураций тестов"""
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
        
        # Конфигурации по умолчанию
        return [
            {
                "name": "pgsql_8k_60r_40w_QD32",
                "parameters": {
                    "ioengine": "libaio",
                    "direct": 1,
                    "rw": "randrw",
                    "rwmixread": 60,
                    "bs": "8k",
                    "iodepth": 32,
                    "size": "1G",
                    "runtime": 120,
                    "filename": "/dev/vdb",
                    "output-format": "json"
                }
            }
        ]
    
    def run_fio_test(self, config: Dict) -> Dict:
        """Запуск одного FIO теста"""
        test_name = config["name"]
        parameters = config["parameters"]
        
        self.logger.info(f"Запуск теста: {test_name}")
        
        # Формирование команды FIO
        cmd = ["fio", "--name", test_name]
        for key, value in parameters.items():
            if key != "name":
                cmd.extend([f"--{key}", str(value)])
        
        try:
            # Запуск FIO
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=parameters.get("runtime", 120) + 30
            )
            
            if result.returncode != 0:
                self.logger.error(f"Ошибка FIO: {result.stderr}")
                return None
            
            # Парсинг JSON вывода
            fio_output = json.loads(result.stdout)
            
            # Обработка результатов
            processed_results = self.process_fio_results(test_name, fio_output)
            
            return processed_results
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Тест {test_name} превысил время выполнения")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка парсинга JSON: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка: {e}")
            return None
    
    def process_fio_results(self, test_name: str, fio_data: Dict) -> Dict:
        """Обработка и извлечение ключевых метрик из результатов FIO"""
        
        # Базовая информация о тесте
        results = {
            "timestamp": datetime.now().isoformat(),
            "test_name": test_name,
            "global_options": fio_data.get("global options", {}),
        }
        
        # Обработка результатов для каждого job
        for job in fio_data.get("jobs", []):
            job_name = job.get("jobname", "unknown")
            read_stats = job.get("read", {})
            write_stats = job.get("write", {})
            
            job_results = {
                "read": {
                    "iops": read_stats.get("iops", 0),
                    "bw_bytes": read_stats.get("bw_bytes", 0),
                    "bw_kbps": read_stats.get("bw_kbps", 0),
                    "latency_ns": {
                        "min": read_stats.get("lat_ns", {}).get("min", 0),
                        "max": read_stats.get("lat_ns", {}).get("max", 0),
                        "mean": read_stats.get("lat_ns", {}).get("mean", 0),
                    },
                    "percentiles": read_stats.get("clat_ns", {}).get("percentile", {}),
                    "latency_us": self.convert_latency_to_us(read_stats.get("lat_ns", {})),
                },
                "write": {
                    "iops": write_stats.get("iops", 0),
                    "bw_bytes": write_stats.get("bw_bytes", 0),
                    "bw_kbps": write_stats.get("bw_kbps", 0),
                    "latency_ns": {
                        "min": write_stats.get("lat_ns", {}).get("min", 0),
                        "max": write_stats.get("lat_ns", {}).get("max", 0),
                        "mean": write_stats.get("lat_ns", {}).get("mean", 0),
                    },
                    "percentiles": write_stats.get("clat_ns", {}).get("percentile", {}),
                    "latency_us": self.convert_latency_to_us(write_stats.get("lat_ns", {})),
                },
                "cpu": job.get("usr_cpu", 0) + job.get("sys_cpu", 0),
            }
            
            results[job_name] = job_results
        
        return results
    
    def convert_latency_to_us(self, latency_ns: Dict) -> Dict:
        """Конвертация наносекунд в микросекунды"""
        return {
            "min": latency_ns.get("min", 0) / 1000,
            "max": latency_ns.get("max", 0) / 1000,
            "mean": latency_ns.get("mean", 0) / 1000,
        }
    
    def save_results(self, results: Dict):
        """Сохранение результатов в файл"""
        if not results:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.output_dir / f"fio_results_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info(f"Результаты сохранены в: {filename}")
        
        # Добавление в историю
        self.history.append(results)
        
        # Сохранение сводной истории
        self.save_history()
    
    def save_history(self):
        """Сохранение полной истории тестов"""
        history_file = self.output_dir / "fio_history.json"
        with open(history_file, 'w') as f:
            json.dump(self.history, f, indent=2)
    
    def generate_report(self):
        """Генерация сводного отчета"""
        if not self.history:
            self.logger.warning("Нет данных для отчета")
            return
        
        report = {
            "generated_at": datetime.now().isoformat(),
            "total_tests": len(self.history),
            "summary": {}
        }
        
        # Агрегация результатов по тестам
        for test_config in self.configs:
            test_name = test_config["name"]
            test_results = [r for r in self.history if r.get("test_name") == test_name]
            
            if test_results:
                report["summary"][test_name] = self.aggregate_test_results(test_results)
        
        # Сохранение отчета
        report_file = self.output_dir / "performance_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Отчет сгенерирован: {report_file}")
        return report
    
    def aggregate_test_results(self, test_results: List[Dict]) -> Dict:
        """Агрегация результатов для одного теста"""
        if not test_results:
            return {}
        
        # Здесь можно добавить сложную логику агрегации
        latest_result = test_results[-1]
        
        return {
            "last_run": latest_result["timestamp"],
            "runs_count": len(test_results),
            "latest_metrics": self.extract_key_metrics(latest_result)
        }
    
    def extract_key_metrics(self, result: Dict) -> Dict:
        """Извлечение ключевых метрик для отчета"""
        metrics = {}
        
        for job_name, job_data in result.items():
            if job_name in ["timestamp", "test_name", "global_options"]:
                continue
            
            metrics[job_name] = {
                "read_iops": job_data["read"]["iops"],
                "write_iops": job_data["write"]["iops"],
                "read_latency_us_p95": job_data["read"]["percentiles"].get("95.000000", 0) / 1000,
                "write_latency_us_p95": job_data["write"]["percentiles"].get("95.000000", 0) / 1000,
                "read_bandwidth_kbps": job_data["read"]["bw_kbps"],
                "write_bandwidth_kbps": job_data["write"]["bw_kbps"],
            }
        
        return metrics
    
    def run_monitoring_cycle(self, interval_minutes: int = 60, cycles: int = None):
        """Запуск периодического мониторинга"""
        self.logger.info(f"Запуск мониторинга с интервалом {interval_minutes} минут")
        
        cycle_count = 0
        try:
            while cycles is None or cycle_count < cycles:
                cycle_count += 1
                self.logger.info(f"Цикл мониторинга #{cycle_count}")
                
                # Запуск всех тестов
                for config in self.configs:
                    results = self.run_fio_test(config)
                    if results:
                        self.save_results(results)
                
                # Генерация отчета после каждого цикла
                self.generate_report()
                
                self.logger.info(f"Ожидание {interval_minutes} минут до следующего цикла...")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            self.logger.info("Мониторинг остановлен пользователем")
        finally:
            self.logger.info("Мониторинг завершен")

def main():
    parser = argparse.ArgumentParser(description="FIO Performance Monitor")
    parser.add_argument("--config", "-c", help="JSON файл с конфигурацией тестов")
    parser.add_argument("--output", "-o", default="fio_results", help="Директория для результатов")
    parser.add_argument("--interval", "-i", type=int, default=60, help="Интервал в минутах")
    parser.add_argument("--cycles", "-n", type=int, help="Количество циклов мониторинга")
    parser.add_argument("--single-run", action="store_true", help="Однократный запуск")
    
    args = parser.parse_args()
    
    # Создание монитора
    monitor = FIOMonitor(args.config, args.output)
    
    if args.single_run:
        # Однократный запуск
        for config in monitor.configs:
            results = monitor.run_fio_test(config)
            if results:
                monitor.save_results(results)
        monitor.generate_report()
    else:
        # Периодический мониторинг
        monitor.run_monitoring_cycle(args.interval, args.cycles)

if __name__ == "__main__":
    main()