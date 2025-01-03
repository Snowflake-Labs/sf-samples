import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter

class OrderGenerator:
    def __init__(
        self,
        # Basic parameters
        start_date='1992-01-01',
        end_date='1998-12-31',
        target_daily_total=100_000_000,
        target_daily_orders=500,
        
        # Trend parameters
        annual_growth_rate=0.15,        # 15% annual growth
        order_value_growth_rate=0.05,   # 5% annual growth in order values
        
        # Seasonal parameters
        holiday_peak_day=350,           # Peak shopping day (Dec 16)
        holiday_effect_magnitude=1.0,   # Strength of holiday effect
        seasonal_baseline=0.8,          # Minimum seasonal multiplier
        seasonal_spread=1000,           # Controls how spread out the holiday effect is
        
        # Weekly parameters
        weekend_dip=0.85,              # Weekend order multiplier
        weekday_boost=1.1,             # Weekday order multiplier
        
        # Value distribution parameters
        pareto_shape=2.0,              # Shape parameter for order values
        min_value_factor=0.3,          # Minimum order value as fraction of average
        value_noise_stddev=0.15,       # Standard deviation for order value noise
        
        # Random seed for reproducibility
        random_seed=None
    ):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.target_daily_total = target_daily_total
        self.target_daily_orders = target_daily_orders
        
        # Store all other parameters
        self.annual_growth_rate = annual_growth_rate
        self.order_value_growth_rate = order_value_growth_rate
        self.holiday_peak_day = holiday_peak_day
        self.holiday_effect_magnitude = holiday_effect_magnitude
        self.seasonal_baseline = seasonal_baseline
        self.seasonal_spread = seasonal_spread
        self.weekend_dip = weekend_dip
        self.weekday_boost = weekday_boost
        self.pareto_shape = pareto_shape
        self.min_value_factor = min_value_factor
        self.value_noise_stddev = value_noise_stddev
        
        # Derived parameters
        self.avg_order_value = target_daily_total / target_daily_orders
        self.min_order_value = self.avg_order_value * self.min_value_factor
        
        if random_seed is not None:
            np.random.seed(random_seed)
    
    def seasonal_effect(self, day_of_year):
        """Stronger effect during holiday season"""
        holiday_effect = np.exp(
            -((day_of_year - self.holiday_peak_day) ** 2) / 
            self.seasonal_spread
        ) * self.holiday_effect_magnitude
        return np.maximum(self.seasonal_baseline + holiday_effect, 0)
    
    def weekly_effect(self, day_of_week):
        """Weekend dips in orders"""
        return self.weekend_dip if day_of_week in [5, 6] else self.weekday_boost
    
    def trend_effect(self, years_passed):
        """Long-term growth trend"""
        return np.power(1 + self.annual_growth_rate, years_passed)
    
    def generate_order_value(self, years_passed):
        """Generate order values following a Pareto distribution"""
        u = np.random.random()
        value = self.min_order_value / np.power(1 - u, 1/self.pareto_shape)
        value = value * np.power(1 + self.order_value_growth_rate, years_passed)
        noise = np.random.normal(1, self.value_noise_stddev)
        return round(value * noise)
    
    def generate_clerk(self):
        """Generate clerk IDs matching TPCH format"""
        clerk_id = np.random.randint(1000)
        return f"Clerk#{clerk_id:09d}"
    
    def generate_customer(self, num_customers=1500):
        """Generate customer IDs matching TPCH format"""
        return f"Customer#{np.random.randint(num_customers):09d}"
    
    def generate_orders(self):
        """Generate supplementary orders with realistic patterns"""
        orders = []
        current_date = self.start_date
        
        while current_date <= self.end_date:
            day_of_year = current_date.dayofyear
            years_passed = (current_date - self.start_date).days / 365
            
            seasonal = self.seasonal_effect(day_of_year)
            weekly = self.weekly_effect(current_date.weekday())
            trend = self.trend_effect(years_passed)
            
            target_orders = round(
                self.target_daily_orders * 
                seasonal * weekly * trend
            )
            
            for _ in range(target_orders):
                order = {
                    'o_orderdate': current_date,
                    'o_totalprice': self.generate_order_value(years_passed),
                    'o_orderstatus': 'O',
                    'o_clerk': self.generate_clerk(),
                    'o_custkey': self.generate_customer()
                }
                orders.append(order)
            
            current_date += timedelta(days=1)
        
        df = pd.DataFrame(orders)
        df = df.sort_values('o_orderdate')
        df['o_orderkey'] = range(len(df))
        df['o_orderkey'] = df['o_orderkey'] + 1_500_000  # Offset to avoid conflicts
        
        return df

def generate_and_save_orders(filename, **generator_params):
    """Generate orders and save to CSV"""
    generator = OrderGenerator(**generator_params)
    df = generator.generate_orders()
    df.to_csv(filename, index=False)
    print(f"Orders saved to {filename}")
    return df

def plot_daily_patterns(filename, figsize=(15, 8), plot_style='compressed'):
    """Load orders from CSV and create visualization"""
    df = pd.read_csv(filename)
    df['o_orderdate'] = pd.to_datetime(df['o_orderdate'])
    
    daily_summary = df.groupby('o_orderdate').agg({
        'o_orderkey': 'count',
        'o_totalprice': 'sum'
    }).reset_index()
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize)
    
    # Plot daily totals
    ax1.plot(daily_summary['o_orderdate'], daily_summary['o_totalprice'], 
            color='blue', linewidth=0.5)
    ax1.set_title('Daily Order Totals')
    ax1.set_ylabel('Daily Total ($)')
    ax1.grid(True, alpha=0.3)
    
    # Set x-axis ticks to show years and months
    ax1.xaxis.set_major_locator(YearLocator())
    ax1.xaxis.set_minor_locator(MonthLocator())
    ax1.xaxis.set_major_formatter(DateFormatter('%Y'))
    ax1.yaxis.set_major_formatter(lambda x, p: f'${x/1e6:.1f}M')
    
    # Plot daily order counts
    ax2.plot(daily_summary['o_orderdate'], daily_summary['o_orderkey'], 
            color='green', linewidth=0.5)
    ax2.set_title('Daily Order Count')
    ax2.set_ylabel('Number of Orders')
    ax2.grid(True, alpha=0.3)
    
    ax2.xaxis.set_major_locator(YearLocator())
    ax2.xaxis.set_minor_locator(MonthLocator())
    ax2.xaxis.set_major_formatter(DateFormatter('%Y'))
    
    for ax in [ax1, ax2]:
        plt.setp(ax.get_xticklabels(), rotation=45)
    
    plt.tight_layout()
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Date Range: {daily_summary['o_orderdate'].min().date()} to {daily_summary['o_orderdate'].max().date()}")
    print(f"Average daily orders: {daily_summary['o_orderkey'].mean():.0f}")
    print(f"Average daily total: ${daily_summary['o_totalprice'].mean():,.2f}")
    
    return fig

if __name__ == "__main__":
    # Example: Generate 2 years of data with pronounced patterns
    params = {
        'start_date': '1992-01-01',
        'end_date': '1998-08-02',
        'target_daily_total': 100_000_000,
        'target_daily_orders': 500,
        'holiday_effect_magnitude': 1.2,
        'weekend_dip': 0.8,
        'annual_growth_rate': 0.15,
        'value_noise_stddev': 0.15
    }
    
    # Generate and save orders
    generate_and_save_orders('supplementary_orders.csv', **params)
    
    # Create visualization
    fig = plot_daily_patterns('supplementary_orders.csv')
    plt.show()
