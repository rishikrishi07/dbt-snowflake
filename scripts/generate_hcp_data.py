#!/usr/bin/env python3
"""
Script to generate synthetic Healthcare Provider (HCP) data for
pharmaceutical field force analytics use case.

This script generates 2 years of data for:
1. HCP basic information
2. Visit/call data
3. Prescription data
4. HCP engagement metrics
"""

import csv
import os
import random
from datetime import datetime, timedelta
import numpy as np
from faker import Faker

# Initialize Faker
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)
np.random.seed(42)

# Constants
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)
NUM_HCPS = 500
NUM_MRS = 50
NUM_TERRITORIES = 10
NUM_PRODUCTS = 5
SPECIALTIES = [
    "Cardiologist", "Endocrinologist", "Gastroenterologist", "Neurologist", 
    "Oncologist", "Psychiatrist", "Pulmonologist", "Rheumatologist", 
    "Primary Care", "Internal Medicine", "Pediatrician", "Geriatrician"
]
STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI", 
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI"
]
PRODUCTS = [
    "Cardiofix", "Diabetreat", "Neuroheal", "Immunoboost", "Painrelief"
]
FACILITIES = [
    "Hospital", "Private Practice", "Clinic", "Medical Center", "Health System"
]
VISIT_TYPES = ["In-person", "Virtual", "Phone call"]
VISIT_OUTCOMES = [
    "Positive", "Negative", "Neutral", "Very Positive", "Very Negative"
]

# Create output directory
os.makedirs("data", exist_ok=True)

# Generate HCP basic information
def generate_hcp_data():
    print("Generating HCP basic information...")
    hcp_data = []
    for i in range(1, NUM_HCPS + 1):
        hcp_id = f"HCP{i:05d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        specialty = random.choice(SPECIALTIES)
        facility = random.choice(FACILITIES)
        years_of_practice = random.randint(1, 35)
        state = random.choice(STATES)
        city = fake.city()
        zip_code = fake.zipcode()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"
        phone = fake.phone_number()
        territory_id = f"T{random.randint(1, NUM_TERRITORIES):02d}"
        
        # Potential score (1-100)
        potential_score = int(np.random.normal(70, 15))
        potential_score = max(1, min(100, potential_score))
        
        # Influence score (1-10)
        influence_score = int(np.random.normal(6, 2))
        influence_score = max(1, min(10, influence_score))
        
        hcp_data.append({
            "hcp_id": hcp_id,
            "first_name": first_name,
            "last_name": last_name,
            "specialty": specialty,
            "facility": facility,
            "years_of_practice": years_of_practice,
            "state": state,
            "city": city,
            "zip_code": zip_code,
            "email": email,
            "phone": phone,
            "territory_id": territory_id,
            "potential_score": potential_score,
            "influence_score": influence_score,
            "created_at": START_DATE.strftime("%Y-%m-%d")
        })
    
    # Write to CSV
    with open("data/hcp_master.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=hcp_data[0].keys())
        writer.writeheader()
        writer.writerows(hcp_data)
    
    return hcp_data

# Generate MR (Medical Representative) data
def generate_mr_data():
    print("Generating Medical Representative (MR) data...")
    mr_data = []
    for i in range(1, NUM_MRS + 1):
        mr_id = f"MR{i:03d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@pharma.com"
        hire_date = fake.date_between(
            start_date=START_DATE - timedelta(days=365*5),
            end_date=START_DATE
        )
        
        # Assign territories to MRs
        territories = []
        num_territories = random.randint(1, 2)
        available_territories = [f"T{j:02d}" for j in range(1, NUM_TERRITORIES + 1)]
        for _ in range(num_territories):
            if available_territories:
                territory = random.choice(available_territories)
                territories.append(territory)
                available_territories.remove(territory)
        
        territory_ids = ",".join(territories)
        
        mr_data.append({
            "mr_id": mr_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "territory_ids": territory_ids
        })
    
    # Write to CSV
    with open("data/medical_representatives.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=mr_data[0].keys())
        writer.writeheader()
        writer.writerows(mr_data)
    
    return mr_data

# Generate visit data
def generate_visit_data(hcp_data, mr_data):
    print("Generating HCP visit data...")
    visit_data = []
    visit_id = 1
    
    # Get territory to MR mapping
    territory_to_mrs = {}
    for mr in mr_data:
        for territory in mr["territory_ids"].split(","):
            if territory in territory_to_mrs:
                territory_to_mrs[territory].append(mr["mr_id"])
            else:
                territory_to_mrs[territory] = [mr["mr_id"]]
    
    # Generate visits for each HCP
    for hcp in hcp_data:
        # Number of visits depends on potential score
        num_visits_per_year = int(hcp["potential_score"] / 20) + 1  # 1-6 visits per year
        
        # Get MRs assigned to this HCP's territory
        territory = hcp["territory_id"]
        mrs_for_territory = territory_to_mrs.get(territory, [])
        if not mrs_for_territory:
            # If no MR assigned, pick a random one
            mrs_for_territory = [random.choice(mr_data)["mr_id"]]
        
        # Generate visits across the 2 years
        current_date = START_DATE
        while current_date <= END_DATE:
            # Add some randomness to visit frequency
            if random.random() < 0.7:  # 70% chance of having the visit
                for _ in range(num_visits_per_year):
                    # Spread visits throughout the year
                    visit_date = current_date + timedelta(days=random.randint(0, 30))
                    if visit_date > END_DATE:
                        break
                    
                    mr_id = random.choice(mrs_for_territory)
                    visit_type = random.choice(VISIT_TYPES)
                    duration_minutes = random.randint(10, 60)
                    outcome = random.choice(VISIT_OUTCOMES)
                    
                    # Engagement score (1-10)
                    engagement_score = int(np.random.normal(6, 2))
                    engagement_score = max(1, min(10, engagement_score))
                    
                    # Add some notes
                    notes = fake.paragraph(nb_sentences=2)
                    
                    # Products discussed (1-3 products)
                    num_products = random.randint(1, min(3, NUM_PRODUCTS))
                    products_discussed = ",".join(random.sample(PRODUCTS, num_products))
                    
                    visit_data.append({
                        "visit_id": f"V{visit_id:06d}",
                        "hcp_id": hcp["hcp_id"],
                        "mr_id": mr_id,
                        "visit_date": visit_date.strftime("%Y-%m-%d"),
                        "visit_type": visit_type,
                        "duration_minutes": duration_minutes,
                        "outcome": outcome,
                        "engagement_score": engagement_score,
                        "products_discussed": products_discussed,
                        "notes": notes
                    })
                    
                    visit_id += 1
                    
            # Move to next month
            current_date = datetime(current_date.year, current_date.month, 1) + timedelta(days=32)
            current_date = datetime(current_date.year, current_date.month, 1)
    
    # Write to CSV
    with open("data/hcp_visits.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=visit_data[0].keys())
        writer.writeheader()
        writer.writerows(visit_data)
    
    return visit_data

# Generate prescription data
def generate_prescription_data(hcp_data, visit_data):
    print("Generating prescription data...")
    prescription_data = []
    
    # Get visit dates and products discussed by HCP
    hcp_visits = {}
    for visit in visit_data:
        hcp_id = visit["hcp_id"]
        if hcp_id not in hcp_visits:
            hcp_visits[hcp_id] = []
        
        visit_info = {
            "date": datetime.strptime(visit["visit_date"], "%Y-%m-%d"),
            "products": visit["products_discussed"].split(",")
        }
        hcp_visits[hcp_id].append(visit_info)
    
    # For each HCP, generate prescription data
    for hcp in hcp_data:
        hcp_id = hcp["hcp_id"]
        
        # Baseline prescription behavior based on potential
        baseline_rxs_per_month = {
            product: random.randint(0, int(hcp["potential_score"] / 20))
            for product in PRODUCTS
        }
        
        # Generate prescriptions for each month
        current_date = START_DATE
        while current_date <= END_DATE:
            month_start = current_date
            month_end = datetime(current_date.year, current_date.month, 28)
            
            # Adjust prescription behavior based on recent visits
            recent_visits = [
                v for v in hcp_visits.get(hcp_id, [])
                if v["date"] > month_start - timedelta(days=60) and v["date"] <= month_start
            ]
            
            # Boost for products discussed in recent visits
            product_boost = {product: 1.0 for product in PRODUCTS}
            for visit in recent_visits:
                for product in visit["products"]:
                    # Boost effect decreases with time
                    days_since_visit = (month_start - visit["date"]).days
                    boost_factor = max(0.1, 1.0 - (days_since_visit / 60))
                    product_boost[product] = max(product_boost.get(product, 1.0), 1.0 + boost_factor)
            
            # Generate monthly prescriptions with some randomness
            for product in PRODUCTS:
                base_rx = baseline_rxs_per_month.get(product, 0)
                boost = product_boost.get(product, 1.0)
                
                # Add seasonality for some products
                season_factor = 1.0
                if product == "Immunoboost" and current_date.month in [11, 12, 1, 2]:
                    # Flu season boost
                    season_factor = 1.5
                elif product == "Neuroheal" and current_date.month in [6, 7, 8]:
                    # Summer boost
                    season_factor = 1.2
                
                # Calculate prescriptions with randomness
                num_prescriptions = int(base_rx * boost * season_factor * random.uniform(0.8, 1.2))
                
                if num_prescriptions > 0:
                    prescription_data.append({
                        "hcp_id": hcp_id,
                        "product": product,
                        "year_month": current_date.strftime("%Y-%m"),
                        "num_prescriptions": num_prescriptions,
                        "total_units": num_prescriptions * random.randint(30, 90),
                    })
            
            # Move to next month
            current_date = datetime(current_date.year, current_date.month, 1) + timedelta(days=32)
            current_date = datetime(current_date.year, current_date.month, 1)
    
    # Write to CSV
    with open("data/prescriptions.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=prescription_data[0].keys())
        writer.writeheader()
        writer.writerows(prescription_data)
    
    return prescription_data

def generate_territory_data():
    print("Generating territory data...")
    territory_data = []
    
    for i in range(1, NUM_TERRITORIES + 1):
        territory_id = f"T{i:02d}"
        territory_name = f"Territory {i}"
        
        # Assign region
        if i <= 3:
            region = "Northeast"
        elif i <= 6:
            region = "Southeast"
        elif i <= 8:
            region = "Midwest"
        else:
            region = "West"
        
        # Assign states (1-3 states per territory)
        num_states = random.randint(1, 3)
        territory_states = random.sample(STATES, num_states)
        states = ",".join(territory_states)
        
        # Set potential value ($)
        potential_value = random.randint(1, 5) * 1000000
        
        territory_data.append({
            "territory_id": territory_id,
            "territory_name": territory_name,
            "region": region,
            "states": states,
            "potential_value": potential_value
        })
    
    # Write to CSV
    with open("data/territories.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=territory_data[0].keys())
        writer.writeheader()
        writer.writerows(territory_data)
    
    return territory_data

def generate_product_data():
    print("Generating product data...")
    product_data = []
    
    for i, product_name in enumerate(PRODUCTS, 1):
        # Define therapeutic areas
        if product_name == "Cardiofix":
            therapeutic_area = "Cardiovascular"
            launch_date = "2018-01-01"
        elif product_name == "Diabetreat":
            therapeutic_area = "Endocrinology"
            launch_date = "2019-05-15"
        elif product_name == "Neuroheal":
            therapeutic_area = "Neurology"
            launch_date = "2021-03-10"
        elif product_name == "Immunoboost":
            therapeutic_area = "Immunology"
            launch_date = "2020-11-22"
        else:  # Painrelief
            therapeutic_area = "Pain Management"
            launch_date = "2017-07-08"
        
        # Set price per unit
        price_per_unit = random.randint(50, 200)
        
        # Set revenue target
        annual_revenue_target = random.randint(10, 50) * 1000000
        
        product_data.append({
            "product_id": f"P{i:02d}",
            "product_name": product_name,
            "therapeutic_area": therapeutic_area,
            "launch_date": launch_date,
            "price_per_unit": price_per_unit,
            "annual_revenue_target": annual_revenue_target
        })
    
    # Write to CSV
    with open("data/products.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=product_data[0].keys())
        writer.writeheader()
        writer.writerows(product_data)
    
    return product_data

def main():
    print("Starting data generation for Pharmaceutical Field Force Analytics...")
    
    # Generate all datasets
    hcp_data = generate_hcp_data()
    mr_data = generate_mr_data()
    territory_data = generate_territory_data()
    product_data = generate_product_data()
    visit_data = generate_visit_data(hcp_data, mr_data)
    prescription_data = generate_prescription_data(hcp_data, visit_data)
    
    print(f"\nData generation complete. Files saved in the 'data' directory:")
    print(f"- HCP master data: {len(hcp_data)} records")
    print(f"- Medical representatives: {len(mr_data)} records")
    print(f"- Territories: {len(territory_data)} records")
    print(f"- Products: {len(product_data)} records")
    print(f"- HCP visits: {len(visit_data)} records")
    print(f"- Prescriptions: {len(prescription_data)} records")

if __name__ == "__main__":
    main() 