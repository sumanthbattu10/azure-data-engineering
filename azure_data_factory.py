"""
Azure Data Factory Pipeline Manager
Author: Sumanth Battu
Description: Azure Data Factory pipeline design and management
             for airline data engineering workflows
"""

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AzureDataFactoryManager:
    """
    Azure Data Factory pipeline manager.
    Designs, deploys, and monitors ADF pipelines
    for airline operations data engineering.
    """

    def __init__(self, subscription_id: str,
                 resource_group: str,
                 factory_name: str):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.credential = DefaultAzureCredential()
        self.client = DataFactoryManagementClient(
            self.credential, self.subscription_id
        )
        logger.info(f"ADF Manager initialized: {factory_name}")

    def create_linked_service_adls(self,
                                   service_name: str,
                                   account_name: str) -> dict:
        """
        Create Azure Data Lake Storage Gen2 linked service
        """
        logger.info(f"Creating ADLS linked service: {service_name}")
        linked_service = {
            "type": "AzureBlobFS",
            "typeProperties": {
                "url": f"https://{account_name}.dfs.core.windows.net",
                "accountKey": {
                    "type": "AzureKeyVaultSecret",
                    "store": {
                        "referenceName": "AzureKeyVaultLinkedService",
                        "type": "LinkedServiceReference"
                    },
                    "secretName": f"{account_name}-key"
                }
            }
        }
        logger.info(f"ADLS linked service created: {service_name}")
        return linked_service

    def create_linked_service_databricks(self,
                                         service_name: str,
                                         workspace_url: str) -> dict:
        """
        Create Azure Databricks linked service
        """
        logger.info(
            f"Creating Databricks linked service: {service_name}"
        )
        linked_service = {
            "type": "AzureDatabricks",
            "typeProperties": {
                "domain": workspace_url,
                "authentication": "MSI",
                "newClusterNodeType": "Standard_DS3_v2",
                "newClusterNumOfWorker": "2:8",
                "newClusterSparkEnvVars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "newClusterSparkConf": {
                    "spark.speculation": "true",
                    "spark.sql.adaptive.enabled": "true"
                }
            }
        }
        logger.info(
            f"Databricks linked service created: {service_name}"
        )
        return linked_service

    def create_pipeline_airline_data(self) -> dict:
        """
        Create ADF pipeline for airline operations data
        Full lifecycle: ingestion → transformation → consumption
        """
        logger.info("Creating airline data pipeline...")
        pipeline = {
            "name": "AirlineDataPipeline",
            "properties": {
                "description": (
                    "End-to-end airline operations data pipeline"
                ),
                "activities": [
                    # Activity 1: Copy from source to ADLS
                    {
                        "name": "CopyFlightData",
                        "type": "Copy",
                        "dependsOn": [],
                        "typeProperties": {
                            "source": {
                                "type": "RestSource",
                                "requestMethod": "GET",
                                "requestInterval": "00.00:00:00.010"
                            },
                            "sink": {
                                "type": "ParquetSink",
                                "storeSettings": {
                                    "type": "AzureBlobFSWriteSettings"
                                }
                            },
                            "enableStaging": False,
                            "dataIntegrationUnits": 8
                        },
                        "inputs": [{"referenceName": "SourceDataset",
                                    "type": "DatasetReference"}],
                        "outputs": [{"referenceName": "ADLSDataset",
                                     "type": "DatasetReference"}]
                    },
                    # Activity 2: Databricks transformation
                    {
                        "name": "TransformWithDatabricks",
                        "type": "DatabricksNotebook",
                        "dependsOn": [
                            {
                                "activity": "CopyFlightData",
                                "dependencyConditions": ["Succeeded"]
                            }
                        ],
                        "typeProperties": {
                            "notebookPath": (
                                "/pipelines/transform_flight_data"
                            ),
                            "baseParameters": {
                                "input_path": {
                                    "value": "@pipeline().parameters"
                                            ".input_path",
                                    "type": "Expression"
                                },
                                "output_path": {
                                    "value": "@pipeline().parameters"
                                            ".output_path",
                                    "type": "Expression"
                                }
                            }
                        },
                        "linkedServiceName": {
                            "referenceName": "DatabricksLinkedService",
                            "type": "LinkedServiceReference"
                        }
                    },
                    # Activity 3: Data quality validation
                    {
                        "name": "ValidateDataQuality",
                        "type": "DatabricksNotebook",
                        "dependsOn": [
                            {
                                "activity": "TransformWithDatabricks",
                                "dependencyConditions": ["Succeeded"]
                            }
                        ],
                        "typeProperties": {
                            "notebookPath": (
                                "/pipelines/validate_data_quality"
                            )
                        },
                        "linkedServiceName": {
                            "referenceName": "DatabricksLinkedService",
                            "type": "LinkedServiceReference"
                        }
                    }
                ],
                "parameters": {
                    "input_path": {"type": "String"},
                    "output_path": {"type": "String"},
                    "pipeline_date": {"type": "String"}
                }
            }
        }
        logger.info("Airline data pipeline created successfully")
        return pipeline

    def create_trigger_schedule(self,
                                trigger_name: str,
                                pipeline_name: str,
                                schedule_hours: int = 1) -> dict:
        """
        Create scheduled trigger for pipeline automation
        """
        logger.info(f"Creating trigger: {trigger_name}")
        trigger = {
            "name": trigger_name,
            "properties": {
                "type": "ScheduleTrigger",
                "typeProperties": {
                    "recurrence": {
                        "frequency": "Hour",
                        "interval": schedule_hours,
                        "startTime": datetime.utcnow().isoformat(),
                        "timeZone": "UTC"
                    }
                },
                "pipelines": [
                    {
                        "pipelineReference": {
                            "type": "PipelineReference",
                            "referenceName": pipeline_name
                        },
                        "parameters": {
                            "pipeline_date": {
                                "value": "@trigger().scheduledTime",
                                "type": "Expression"
                            }
                        }
                    }
                ]
            }
        }
        logger.info(f"Trigger created: {trigger_name}")
        return trigger

    def monitor_pipeline_runs(self,
                              pipeline_name: str,
                              days: int = 7) -> list:
        """
        Monitor pipeline runs and report status
        """
        logger.info(
            f"Monitoring pipeline runs: {pipeline_name}"
        )
        filter_params = RunFilterParameters(
            last_updated_after=datetime.utcnow() - timedelta(days=days),
            last_updated_before=datetime.utcnow()
        )
        runs = self.client.pipeline_runs.query_by_factory(
            self.resource_group,
            self.factory_name,
            filter_params
        )

        results = []
        for run in runs.value:
            if run.pipeline_name == pipeline_name:
                results.append({
                    "run_id": run.run_id,
                    "status": run.status,
                    "start_time": str(run.run_start),
                    "duration_seconds": run.duration_in_ms / 1000
                    if run.duration_in_ms else 0,
                    "message": run.message
                })

        succeeded = sum(
            1 for r in results if r["status"] == "Succeeded"
        )
        failed = sum(
            1 for r in results if r["status"] == "Failed"
        )
        logger.info(
            f"Pipeline runs — Succeeded: {succeeded}, "
            f"Failed: {failed}"
        )
        return results


if __name__ == "__main__":
    # Configuration
    config = {
        "subscription_id": "your-subscription-id",
        "resource_group": "airline-data-rg",
        "factory_name": "airline-data-factory"
    }

    manager = AzureDataFactoryManager(**config)

    # Create pipeline
    pipeline = manager.create_pipeline_airline_data()
    print("\nPipeline Configuration:")
    print(json.dumps(pipeline, indent=2))

    # Create trigger
    trigger = manager.create_trigger_schedule(
        "HourlyTrigger", "AirlineDataPipeline", 1
    )
    print("\nTrigger Configuration:")
    print(json.dumps(trigger, indent=2))
