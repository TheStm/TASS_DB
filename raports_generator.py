import matplotlib
from neo4j import GraphDatabase
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import scipy as sp
# Use non-interactive backend for saving files
matplotlib.use("Agg")

# ==========================================
# FUNCTION 1: Generate CSV Report
# ==========================================
def generate_country_connection_report_csv(uri, user, password, year="2017"):
    """
    Connects to Neo4j, aggregates flight connections between countries for a specific year,
    and saves the report to a CSV file.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))

    # Query aggregates flights by origin and destination country
    query = """
    MATCH (f:Flight)-[:ON_DAY]->(d:Day)
    WHERE toString(d.date) STARTS WITH $year_str
    MATCH (f)-[:DEPARTS_FROM]->(dep:Airport)-[:IN_CITY]->(:City)-[:IN_COUNTRY]->(depCo:Country),
          (f)-[:ARRIVES_TO]->(arr:Airport)-[:IN_CITY]->(:City)-[:IN_COUNTRY]->(arrCo:Country)
    RETURN depCo.name AS origin_country, 
           arrCo.name AS destination_country, 
           count(f) AS flights
    ORDER BY origin_country, flights DESC
    """

    print(f"--- [Step 1] Generating Country Connection Report for: {year} ---")
    try:
        with driver.session() as session:
            result = session.run(query, year_str=str(year))
            data = [r.data() for r in result]
            df = pd.DataFrame(data)
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return None
    finally:
        driver.close()

    if df.empty:
        print("❌ Error: No flight data found for this year.")
        return None

    # Save with a descriptive filename
    output_csv = f"reports/report_country_connections_{year}.csv"
    df.to_csv(output_csv, index=False)
    print(f"✅ Report saved to: {output_csv}")
    return output_csv


# ==========================================
# FUNCTION 2: Visualize Report (Heatmap)
# ==========================================
def visualize_country_connections_heatmap(input_csv, year="2017"):
    """
    Reads the country connection CSV report, filters for Europe, translates to Polish,
    and generates a heatmap visualization.
    """
    print(f"--- [Step 2] Visualizing Country Connections for: {year} ---")

    # 1. Load Data
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"❌ Error: Report file {input_csv} not found.")
        return

    # 2. Define Europe Filter
    europe_filter = [
        "Albania", "Andorra", "Austria", "Belarus", "Belgium", "Bosnia and Herzegovina",
        "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark", "Estonia", "Finland",
        "France", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo",
        "Latvia", "Liechtenstein", "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco",
        "Montenegro", "Netherlands", "North Macedonia", "Norway", "Poland", "Portugal",
        "Romania", "Russia", "San Marino", "Serbia", "Slovakia", "Slovenia", "Spain",
        "Sweden", "Switzerland", "Ukraine", "United Kingdom", "Vatican City"
    ]

    # 3. Filter Data
    df = df[df['origin_country'].isin(europe_filter) & df['destination_country'].isin(europe_filter)]

    if df.empty:
        print("❌ Error: No European connections found in the report.")
        return

    # 4. Polish Translation
    pl_names = {
        "United Kingdom": "Wielka Brytania", "Germany": "Niemcy", "France": "Francja",
        "Spain": "Hiszpania", "Italy": "Włochy", "Poland": "Polska", "Ireland": "Irlandia",
        "Netherlands": "Holandia", "Belgium": "Belgia", "Switzerland": "Szwajcaria",
        "Austria": "Austria", "Portugal": "Portugalia", "Greece": "Grecja", "Sweden": "Szwecja",
        "Norway": "Norwegia", "Denmark": "Dania", "Finland": "Finlandia", "Russia": "Rosja",
        "Ukraine": "Ukraina", "Czech Republic": "Czechy", "Hungary": "Węgry", "Turkey": "Turcja",
        "Romania": "Rumunia", "Bulgaria": "Bułgaria", "Croatia": "Chorwacja", "Slovakia": "Słowacja",
        "Lithuania": "Litwa", "Latvia": "Łotwa", "Estonia": "Estonia", "Belarus": "Białoruś",
        "Iceland": "Islandia", "Cyprus": "Cypr", "Malta": "Malta", "Luxembourg": "Luksemburg",
        "Slovenia": "Słowenia", "Serbia": "Serbia", "Bosnia and Herzegovina": "Bośnia i Hercegowina",
        "Montenegro": "Czarnogóra", "Albania": "Albania", "North Macedonia": "Macedonia Północna",
        "Moldova": "Mołdawia"
    }

    df['origin_country'] = df['origin_country'].map(pl_names).fillna(df['origin_country'])
    df['destination_country'] = df['destination_country'].map(pl_names).fillna(df['destination_country'])

    # 5. Create Matrix
    matrix = df.pivot(index='origin_country', columns='destination_country', values='flights')
    matrix = matrix.fillna(0)
    matrix = matrix.sort_index(axis=0).sort_index(axis=1)

    # 6. Robust Scaling
    robust_max = np.percentile(matrix.values, 98)

    # 7. Plotting
    plt.figure(figsize=(20, 18))
    sns.set(font_scale=1.0)

    ax = sns.heatmap(
        matrix,
        cmap="Spectral_r",
        vmax=robust_max,
        linewidths=0.5,
        linecolor='#f0f0f0',
        annot=True,
        fmt=".0f",
        annot_kws={"size": 8},
        cbar_kws={'label': 'Liczba Lotów'}
    )

    plt.title(f"Raport Połączeń Międzykrajowych ({year})", fontsize=18, pad=20, fontweight='bold')
    plt.xlabel("Kraj Przylotu", fontsize=14, labelpad=10, fontweight='bold')
    plt.ylabel("Kraj Wylotu", fontsize=14, labelpad=10, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()

    output_img = f"reports/heatmap_country_connections_{year}.png"
    plt.savefig(output_img, dpi=300)
    plt.close()
    print(f"Visualization saved: {output_img}")


# ==========================================
# FUNCTION 1: Generate Monthly Report (CSV)
# ==========================================
def generate_monthly_flight_report(uri, user, password, year="2017"):
    """
    Queries Neo4j for flight data in a specific year.
    Returns a CSV with columns: [month, origin, destination, flights]
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))

    # We extract the month using substring(toString(d.date), 5, 2)
    # Assumes date format "YYYY-MM-DD"
    query = """
    MATCH (f:Flight)-[:ON_DAY]->(d:Day)
    WHERE toString(d.date) STARTS WITH $year_str
    MATCH (f)-[:DEPARTS_FROM]->(dep:Airport)-[:IN_CITY]->(:City)-[:IN_COUNTRY]->(depCo:Country),
          (f)-[:ARRIVES_TO]->(arr:Airport)-[:IN_CITY]->(:City)-[:IN_COUNTRY]->(arrCo:Country)
    RETURN substring(toString(d.date), 5, 2) AS month,
           depCo.name AS origin_country, 
           arrCo.name AS destination_country, 
           count(f) AS flights
    ORDER BY month, origin_country, flights DESC
    """

    print(f"--- Generating Monthly Report for Year: {year} ---")
    try:
        with driver.session() as session:
            result = session.run(query, year_str=str(year))
            data = [r.data() for r in result]
            df = pd.DataFrame(data)
    except Exception as e:
        print(f"Database Error: {e}")
        return None
    finally:
        driver.close()

    if df.empty:
        print("Error: No data found.")
        return None

    # Save to CSV
    output_csv = f"reports/monthly_flight_report_{year}.csv"
    df.to_csv(output_csv, index=False)
    print(f"Report saved to: {output_csv}")
    print(f"   (Contains {len(df)} rows of monthly route data)")
    return output_csv


# ==========================================
# FUNCTION 2: Print Top 5 Stats for a Country
# ==========================================
def print_country_stats(csv_file, country_name):
    """
    Reads the monthly report CSV and prints the Top 5 Destinations
    and Top 5 Incoming sources for the specified country (aggregated over the whole year).
    """
    print(f"\n --- Analysis for Country: {country_name.upper()} ---")

    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f" Error: File {csv_file} not found.")
        return

    # 1. Top 5 Destinations (Where did planes fly TO from this country?)
    # Filter: Origin is the specific country
    outgoing = df[df['origin_country'] == country_name]

    if outgoing.empty:
        print(f"   No outgoing flights found for {country_name}.")
    else:
        # Group by destination and sum up flights (across all months)
        top_dest = outgoing.groupby('destination_country')['flights'].sum().sort_values(ascending=False).head(5)
        print("\n TOP 5 DESTINATIONS (Departing from " + country_name + "):")
        for rank, (dest, count) in enumerate(top_dest.items(), 1):
            print(f"   {rank}. {dest}: {count} flights")

    # 2. Top 5 Incoming (Where did planes arrive FROM?)
    # Filter: Destination is the specific country
    incoming = df[df['destination_country'] == country_name]

    if incoming.empty:
        print(f"\n   No incoming flights found for {country_name}.")
    else:
        # Group by origin and sum up flights
        top_in = incoming.groupby('origin_country')['flights'].sum().sort_values(ascending=False).head(5)
        print("\n TOP 5 INCOMING SOURCES (Arriving in " + country_name + "):")
        for rank, (origin, count) in enumerate(top_in.items(), 1):
            print(f"   {rank}. {origin}: {count} flights")

    print("-" * 50)


# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":

    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PASSWORD = "password"

    # You can now easily call this function
    # generate_country_connection_report_csv(URI, USER, PASSWORD, year="2017")
    # generate_monthly_flight_report(URI, USER, PASSWORD, year="2018")

    # 2. Use the report to analyze
    visualize_country_connections_heatmap("reports/report_country_connections_2017.csv", "2017")
    # print_country_stats("reports/monthly_flight_report_2018.csv", "Lebanon")
