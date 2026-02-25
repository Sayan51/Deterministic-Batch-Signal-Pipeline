import sys
import os
import json
import logging
import argparse
import time
import traceback
import yaml
import pandas as pd
import numpy as np

class PipelineError(Exception):
    pass

class ConfigError(PipelineError):
    pass

class DataValidationError(PipelineError):
    pass

class ProcessingError(PipelineError):
    pass

class MetricsWriteError(PipelineError):
    pass

def setup_logging(log_file="run.log"):
    log_format = "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stderr)
        ]
    )
    return logging.getLogger("Pipeline")

def load_config(config_path, logger):
    logger.info(f"Loading config from {config_path}")
    if not os.path.exists(config_path):
        raise ConfigError(f"Config file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Malformed YAML: {e}")

    required_keys = {'seed', 'window', 'version'}
    if config is None:
        raise ConfigError("Config is empty")
    
    actual_keys = set(config.keys())
    if not required_keys.issubset(actual_keys):
        missing = required_keys - actual_keys
        raise ConfigError(f"Missing required config keys: {missing}")
    
    if actual_keys != required_keys:
        extra = actual_keys - required_keys
        raise ConfigError(f"Extra keys in config: {extra}")

    if not isinstance(config['seed'], int):
        raise ConfigError(f"seed must be an int, got {type(config['seed']).__name__}")
    if not isinstance(config['window'], int) or config['window'] <= 0:
        raise ConfigError(f"window must be a positive int, got {config['window']}")
    if not isinstance(config['version'], str) or not config['version']:
        raise ConfigError(f"version must be a non-empty string")

    logger.info("Config validation success")
    logger.info(f"Validated config: seed={config['seed']}, window={config['window']}, version={config['version']}")
    return config

def validate_data(data_path, logger):
    logger.info(f"Validating dataset from {data_path}")
    if not os.path.exists(data_path):
        raise DataValidationError(f"Data file not found: {data_path}")
    
    if not os.access(data_path, os.R_OK):
        raise DataValidationError(f"Data file not readable: {data_path}")

    try:
        df = pd.read_csv(data_path)
    except Exception as e:
        raise DataValidationError(f"Failed to read CSV: {e}")

    if df.empty:
        raise DataValidationError("Dataset is empty")

    if 'close' not in df.columns:
        raise DataValidationError("Required column 'close' missing")

    if not pd.api.types.is_numeric_dtype(df['close']):
        raise DataValidationError("'close' column must be numeric")

    logger.info(f"Rows loaded: {len(df)}")
    logger.info("Dataset schema validation success")
    return df

def process_pipeline(df, config, logger):
    window = config['window']
    seed = config['seed']

    np.random.seed(seed)
    
    logger.info("Processing steps: Rolling mean start")
    df_proc = df.copy()
    df_proc['rolling_mean'] = df_proc['close'].rolling(window=window).mean()
    logger.info("Processing steps: Rolling mean end")

    logger.info("Processing steps: Signal generation start")
    df_proc['signal'] = 0
    mask = df_proc['rolling_mean'].notna()
    df_proc.loc[mask, 'signal'] = (df_proc.loc[mask, 'close'] > df_proc.loc[mask, 'rolling_mean']).astype(int)
    logger.info("Processing steps: Signal generation end")

    return df_proc

def write_metrics(metrics, output_path, logger):
    try:
        with open(output_path, 'w') as f:
            json.dump(metrics, f, indent=2, sort_keys=True)
        logger.info(f"Metrics summary written to {output_path}")
    except Exception as e:
        raise MetricsWriteError(f"Failed to write metrics: {e}")

def generate_metrics(df_proc, config, start_time, status="success", error_message=None):
    latency_ms = int((time.time() - start_time) * 1000)
    
    if status == "success":
        rows_processed = len(df_proc)
        signal_rate = float(df_proc['signal'].mean())
        return {
            "version": config["version"],
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": config["seed"],
            "status": "success"
        }
    else:
        return {
            "version": config.get("version", "v1") if config else "v1",
            "status": "error",
            "error_message": str(error_message)
        }

def main():
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Deterministic Batch Signal Pipeline")
    parser.add_argument("--input", default="data.csv", help="input data file")
    parser.add_argument("--config", default="config.yaml", help="config file")
    parser.add_argument("--output", default="metrics.json", help="output metrics file")
    parser.add_argument("--log-file", default="run.log", help="log file")
    args = parser.parse_args()

    logger = setup_logging(args.log_file)

    logger.info("Job start")
    logger.info(f"CLI arguments: {args}")

    config = None
    try:
        config = load_config(args.config, logger)
        df = validate_data(args.input, logger)
        df_proc = process_pipeline(df, config, logger)
        
        metrics = generate_metrics(df_proc, config, start_time)
        write_metrics(metrics, args.output, logger)
        
        print(json.dumps(metrics, indent=2, sort_keys=True))
        
        logger.info("Job end | status: success")
        sys.exit(0)

    except PipelineError as e:
        logger.error(f"Pipeline error: {e}")
        metrics = generate_metrics(None, config, start_time, status="error", error_message=e)
        write_metrics(metrics, args.output, logger)
        print(json.dumps(metrics, indent=2, sort_keys=True))
        logger.info("Job end | status: failure")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
        metrics = generate_metrics(None, config, start_time, status="error", error_message=str(e))
        write_metrics(metrics, args.output, logger)
        print(json.dumps(metrics, indent=2, sort_keys=True))
        logger.info("Job end | status: failure")
        sys.exit(1)

if __name__ == "__main__":
    main()
