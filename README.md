"# Financial-Loan-using-Medallion-Architecture-Streaming" 
"# Financial-loan-analysis-using-medallion-architecture" 
financial-loan-analysis/
├── databricks.yml                    # Main bundle configuration
├── resources/
│   ├── bronze_job.yml               # Bronze layer workflow
│   ├── silver_job.yml               # Silver layer workflow
│   └── gold_job.yml                 # Gold layer workflow
├── src/
│   ├── bronze_ingestion.py          # Bronze ingestion logic
│   ├── silver_transformation.py     # Silver transformation logic
│   ├── gold_feature_layer.py        # Gold feature engineering
│   └── gold_aggregation_layer.py    # Gold aggregations
└── README.md
