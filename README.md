

<p align="left">
<img alt="License" src="https://img.shields.io/badge/License-MIT-blue.svg">
<img alt="Python" src="https://img.shields.io/badge/Python-3.9%2B-blueviolet">
<img alt="Models" src="https://img.shields.io/badge/Models-Ensemble_ML_Methods-4285F4">
<img alt="Framework" src="https://img.shields.io/badge/Framework-Scikit--learn-orange">
<img alt="Framework" src="https://img.shields.io/badge/Visulisation-Matplotlib-green">
<img alt="RStudio" src="https://img.shields.io/badge/RStudio-4285F4?style=flat&logo=rstudio&logoColor=white">
</p>

# Predicting Phytoplankton Dynamics with Explainable AI

This project uses machine learning and explainable AI (XAI) to predict and understand phytoplankton dynamics in Falling Creek and Beaverdam Reservoirs in Virginia, USA. By analyzing environmental variables collected at the sites, we uncover the hidden patterns driving phytoplankton dynamics, offering insights for both water quality management and ecological research. 

---

## 📁 Project Structure


### Step 1: Fetch the Data

To begin the analysis, you first need to compile the dataset from the Virginia LTREB Reservoirs monitoring program. You can do this by following steps below:

  - Run the R code from the GitHub Repository (https://github.com/abreefpilz/Reservoirs/blob/master/Scripts/Daily_avg_RS.R)

The script automatically fetches the required data packages directly from the EDI Repository API portal (given below), processes the variables, and saves the final output as a .csv file.

Citations:

Carey, C. C., & Breef-Pilz, A. (2025). Time series of high-frequency meteorological data at Falling Creek Reservoir, Virginia, USA, 2015–2024 (ver. 9). Environmental Data Initiative. https://doi.org/10.6073/pasta/0389840ddcb39ec5526869ac898ddb5d <sub>Accessed 2025-09-17</sub>.

Carey, C. C., & Breef-Pilz, A. (2025). Time series of high-frequency sensor data measuring water temperature, dissolved oxygen, conductivity, specific conductance, total dissolved solids, chlorophyll a, phycocyanin, fluorescent dissolved organic matter, and turbidity at discrete depths, and water level in Beaverdam Reservoir, Virginia, USA, 2009–2024 (ver. 5). Environmental Data Initiative. https://doi.org/10.6073/pasta/8f666b34c120aa5d2242964cf3147f90 <sub>Accessed 2025-09-17</sub>.

Carey, C. C., & Breef-Pilz, A. (2025). Time series of high-frequency sensor data measuring water temperature, dissolved oxygen, pressure, conductivity, specific conductance, total dissolved solids, chlorophyll a, phycocyanin, fluorescent dissolved organic matter, and turbidity at discrete depths in Falling Creek Reservoir, Virginia, USA, 2018–2024 (ver. 9). Environmental Data Initiative. https://doi.org/10.6073/pasta/f74fd5c5fc9ea4141a662ce773962ce0 <sub>Accessed 2025-09-17</sub>.


Running this script will reproduce the FCR_BVR_Met_daily_obs_2019_2024.csv used for this project (uploaded here). 


### Step 2: Data Analysis and Visualization

Once the data are retrieved/loaded, the next step is to perform an exploratory data analysis (EDA). 

- [chl-a_timeseries.ipynb](https://github.com/rohitshukla01/XAI_Manuscript_Analyses/blob/main/Chl-a_timeseries.ipynb)


 This script processes the phytoplankton (chlorophyll-a) data, visualizes trends, and generates plots to understand the dataset's characteristics.

 ### Step 3: ML-XAI implemenation

The machine learning and explainable AI analyses are performed in two separate Jupyter Notebooks, with one dedicated to each reservoir as descibed below:

  - Falling Creek Reservoir (FCR)

  - [XAI_FCR_ANALYSES.ipynb](https://github.com/rohitshukla01/XAI_Manuscript_Analyses/blob/main/XAI_FCR_ANALYSES.ipynb)


Contains the complete ML models and SHAP analyses with figures for the FCR.

  -  Beaverdam Reservoir (BVR)

  - [XAI_BVR_ANALYSES.ipynb](https://github.com/rohitshukla01/XAI_Manuscript_Analyses/blob/main/XAI_BVR_ANALYSES.ipynb)


Contains the complete ML models and SHAP analyses with figures for the BVR.

---

## Bathymetry Data Processing & Visualization

The bathymetry figures for each reservoir are generated using the notebook below.

- [bathymetry_bvr_fcr.ipynb](https://github.com/rohitshukla01/XAI_Manuscript_Analyses/blob/main/bathymetry_bvr_fcr.ipynb)


---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/rohitshukla01/XAI_Manuscript_Analyses.git
cd XAI_Manuscript_Analyses
```

---

## ✅ Citation & Data Access

📑 If you use this code in a publication, cite this repository.

🌐 Visit EDI portal for direct data acquisition: https://portal.edirepository.org/nis/home.jsp










