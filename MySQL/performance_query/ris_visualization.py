import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Step 1: Load the CSV file
def load_data(filename):
    try:
        # Load the CSV file into a DataFrame
        data = pd.read_csv(filename, header=None, names=["Query", "ExecutionTime"])
        print("Data loaded successfully.")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Step 2: Visualize the data with a custom hybrid time scale
def visualize_data(data):
    # Create a figure
    plt.figure(figsize=(12, 6))

    # Scatter plot for each query
    for query, group in data.groupby("Query"):
        plt.scatter([query] * len(group), group["ExecutionTime"], label=query, alpha=0.7)

    # Custom ticks for the y-axis
    small_ticks = [0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4]  # Fine-grained for small values
    large_ticks = [1, 10, 100, 200, 300]  # Coarser for large values
    all_ticks = small_ticks + large_ticks
    all_ticks = sorted(all_ticks)  # Sort the ticks

    # Set the y-axis to a logarithmic scale
    plt.yscale("log")

    # Customize the y-axis ticks and labels
    plt.yticks(all_ticks, [f"{t} sec" for t in all_ticks])

    # Add labels and title
    plt.title("Query Execution Times with Hybrid Time Scale")
    plt.xlabel("Query")
    plt.ylabel("Execution Time (seconds)")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Show the plot
    plt.legend(title="Queries", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.show()

# Step 3: Main function
def main():
    # File path
    filename = "query_execution_times.csv"

    # Load the data
    data = load_data(filename)
    if data is None:
        return

    # Visualize the data
    visualize_data(data)

if __name__ == "__main__":
    main()