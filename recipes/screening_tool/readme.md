# 🌍 AGOL Survey Tools

This Python script is designed to interact with ArcGIS Online (AGOL) to manage survey data and perform spatial data processing and analysis. The main functionalities include establishing connections to AGOL, managing survey features, and executing spatial analyses. Perfect for geospatial enthusiasts and professionals!

## 📝 Author
- Eric Millan

## 📅 Last Updated
- April 16, 2024

## ✨ Features
- 🌐 Connect to AGOL using specified credentials.
- ✔️ Validate and manage AGOL items and layers.
- 📊 Handle individual survey submissions.
- 🗺️ Prepare and process spatial data.
- 📈 Generate and manage spatial analysis reports and distributions.

## 🛠 Installation

1. Ensure Python is installed on your machine.
2. Clone the repository:

git clone <repository-url>

3. Navigate to the script directory:

cd path/to/script
  
## 🚀 Usage

To use the script, you need to have valid credentials for AGOL and optionally for Oracle for database connections. Set up the necessary environment variables or modify the script to include secure handling of credentials.

To run the script:

python agol_survey_tools.py

## 📦 Dependencies
- ArcPy (Only used in MAMU Analysis)
- ArcGIS API for Python
- NumPy
- Matplotlib
- PIL
- Seaborn
- SciPy
- GeoPandas
- Pandas
- Fiona
- OSgeo (GDAL)

Ensure all dependencies are installed using pip or conda, depending on your environment setup.

## ⚙️ Configuration

Modify the script paths and credentials as necessary to match your AGOL setup and local environment.

## 🤝 Contributing

Contributions are welcome. Please fork the repository and submit a pull request with your suggested changes.
