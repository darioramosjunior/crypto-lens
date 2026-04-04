import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from dotenv import load_dotenv
from discord_integrator import upload_to_discord
import numpy as np
from scipy.stats import gaussian_kde

load_dotenv()

script_dir = os.path.dirname(os.path.abspath(__file__))
market_cap_csv = os.path.join(script_dir, "market_cap_data.csv")
output_dir = os.path.join(script_dir, "hourly_market_pulse")

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read Discord webhook from environment
discord_webhook_url = os.getenv("MARKET_CAP_HEALTH_WEBHOOK")
if not discord_webhook_url:
    print("Warning: MARKET_CAP_HEALTH_WEBHOOK not set in .env file. Chart will be generated but not uploaded to Discord.")

def create_market_cap_health_chart():
    """
    Creates a box plot showing day_change_percent distribution for each market cap category.
    """
    try:
        # Read the market cap data
        df = pd.read_csv(market_cap_csv)
        df.columns = df.columns.str.strip()
        
        # Remove rows with empty category
        df = df[(df['category'].notna()) & (df['category'] != '')]
        
        # Convert day_change_percent to numeric (handle any non-numeric values)
        df['day_change_percent'] = pd.to_numeric(df['day_change_percent'], errors='coerce')
        df = df.dropna(subset=['day_change_percent'])
        
        if df.empty:
            print("No valid data found for chart creation.")
            return
        
        # Define category order for consistent display
        category_order = ['Large Cap', 'Mid Cap', 'Small Cap']
        
        # Prepare data for each category
        large_cap_data = df[df['category'] == 'Large Cap']['day_change_percent'].values
        mid_cap_data = df[df['category'] == 'Mid Cap']['day_change_percent'].values
        small_cap_data = df[df['category'] == 'Small Cap']['day_change_percent'].values
        
        data_by_category = [large_cap_data, mid_cap_data, small_cap_data]
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Color palette for categories
        colors = ['#66c2a5', '#fc8d62', '#8da0cb']
        
        # Create raincloud plots
        for i, (cat, data) in enumerate([(cat, data_by_category[j]) for j, cat in enumerate(category_order)], 1):
            data = data_by_category[i-1]
            
            # Add individual points with jitter
            x_jitter = np.random.normal(i, 0.04, size=len(data))
            ax.scatter(x_jitter, data, alpha=0.5, s=50, color=colors[i-1], 
                      edgecolors='black', linewidth=0.5, zorder=2)
            
            # Create density curve above
            if len(data) > 1:
                try:
                    kde = gaussian_kde(data)
                    y_range = np.linspace(data.min() - 1, data.max() + 1, 200)
                    density = kde(y_range)
                    # Normalize density to fit nicely
                    density = density / density.max() * 0.3
                    ax.fill_betweenx(y_range, i - density, i + density, 
                                    alpha=0.6, color=colors[i-1], zorder=1)
                except Exception:
                    pass
            
            # Add mean and median lines
            mean_val = data.mean()
            median_val = np.median(data)
            ax.hlines(mean_val, i - 0.15, i + 0.15, colors='red', linestyles='--', 
                     linewidth=2, label='Mean' if i == 1 else '', zorder=3)
            ax.hlines(median_val, i - 0.15, i + 0.15, colors='darkblue', linestyles='-', 
                     linewidth=2, label='Median' if i == 1 else '', zorder=3)
        
        # Customize the plot
        ax.set_xticks(range(1, len(category_order) + 1))
        
        # Create labels with counts
        counts = [len(large_cap_data), len(mid_cap_data), len(small_cap_data)]
        labels_with_counts = [f"{cat}\n(n={count})" for cat, count in zip(category_order, counts)]
        ax.set_xticklabels(labels_with_counts)
        
        ax.set_title('Market Cap Category Health - Day Change % Distribution (Raincloud Plot)', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Market Cap Category', fontsize=12, fontweight='bold')
        ax.set_ylabel('Day Change %', fontsize=12, fontweight='bold')
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Add legend
        ax.legend(loc='upper right', fontsize=10)
        
        # Adjust layout
        plt.tight_layout()
        
        # Save the chart
        output_file = os.path.join(output_dir, "market_cap_health.png")
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Chart saved to {output_file}")
        
        # Upload to Discord if webhook is configured
        if discord_webhook_url:
            try:
                upload_to_discord(discord_webhook_url, image_path=output_file)
                print("Chart uploaded to Discord successfully!")
            except Exception as e:
                print(f"Warning: Failed to upload chart to Discord: {e}")
        
        # Print statistics
        print("\n=== Market Cap Category Statistics ===")
        data_dict = {
            'Large Cap': large_cap_data,
            'Mid Cap': mid_cap_data,
            'Small Cap': small_cap_data
        }
        
        for category in category_order:
            cat_data = data_dict[category]
            print(f"\n{category}:")
            print(f"  Count: {len(cat_data)}")
            print(f"  Mean: {cat_data.mean():.2f}%")
            print(f"  Median: {pd.Series(cat_data).median():.2f}%")
            print(f"  Std Dev: {cat_data.std():.2f}%")
            print(f"  Min: {cat_data.min():.2f}%")
            print(f"  Max: {cat_data.max():.2f}%")
        
        plt.close()
        
    except Exception as e:
        print(f"Error creating market cap health chart: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print(f"Running market_cap_health.py at {datetime.now()}...")
    create_market_cap_health_chart()
    print("Completed!")
