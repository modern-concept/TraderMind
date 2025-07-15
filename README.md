# TraderMind

**Status**: 🚧 In Development  
**Author**: Danillo Xavier  
**Goal**: To automate Forex market analysis, classification, and decision-making using Machine Learning, GPU acceleration, and cloud deployment.

---

## 📌 Overview

**TraderMind** is a fully autonomous AI system for real-time analysis and classification of Forex financial data. It covers:

- Automated data collection
- Technical indicator calculation (fully in Python)
- ML training using Ray with CUDA acceleration
- Flask API for serving predictions
- CI/CD pipelines for automated deployment
- Cloud-ready for AWS EC2, Google Cloud Platform (GCP), and Azure

---

## 🧰 Technologies Used

| Layer                | Technologies                                                   |
|----------------------|----------------------------------------------------------------|
| Language             | Python 3.11                                                    |
| Data Processing      | Spark, Pandas, DuckDB                                          |
| Parallelization      | Ray, CUDA                                                      |
| Machine Learning     | Scikit-learn / PyTorch                                         |
| API                  | Flask                                                          |
| Deployment           | Docker, GitHub Actions (CI/CD)                                 |
| Cloud Infrastructure | AWS EC2 (current), GCP and Azure (planned)                    |
| Frontend Companion   | React (ClassPlanner project for UI and planning interface)     |

---

## 🚀 Features

- [ ] Automated Forex data collection
- [ ] Technical indicators calculated in Python
- [ ] ML model training pipeline
- [ ] REST API to serve predictions
- [ ] Migrate all indicators from JavaScript to Python
- [ ] Implement Spark for efficient sorting and filtering
- [ ] Add GitHub CI/CD workflow for tests and deploy
- [ ] Dockerize and deploy on GCP/Azure
- [ ] Full MLOps pipeline with automatic weekly retraining

---

## 📂 Project Structure
tradermind/
│
├── data/ # Raw and processed data
├── indicators/ # Technical indicators in Python
├── ml/ # Training scripts and model files
├── api/ # Flask-based prediction API
├── pipelines/ # Orchestration and automation
├── legacy/ # legacy javascript code
├── Dockerfile # Docker image definition
├── requirements.txt # Python dependencies
└── README.md
