import pandas as pd

df = pd.read_parquet('data/staging/trips.parquet')
df['calculated'] = df['fare'] + df['tips'] + df['tolls'] + df['extras'] + 0.50
df['diff'] = (df['trip_total'] - df['calculated']).abs()

inconsistent = df[df['diff'] > 0.10]
print(f"Total inconsistentes con +$0.50: {len(inconsistent):,}")
print(f"\nDistribución de diffs:")
print(inconsistent['diff'].round(2).value_counts().head(10))

# Ver si hay viajes donde la diff ahora es -0.50 (sobrecompensamos)
over = df[df['trip_total'] - df['calculated'] < -0.10]
print(f"\nViajes donde sumamos $0.50 de más: {len(over):,}")