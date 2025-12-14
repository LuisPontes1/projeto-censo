import pandas as pd
import re

DIAMOND_FILE = r"c:\projetos\projeto-censo\data\diamond\censo_2022_diamond_features_final.parquet"

df = pd.read_parquet(DIAMOND_FILE)
cols = df.columns.tolist()

# 1. Check Gold vs Diamond relationship
# We assume Gold columns are those without spatial suffixes (_lag, _lisa, etc) and not gravity
spatial_suffixes = ['_lag_k5', '_hetero_k5', '_inequality_k5', '_isolation_k5', '_rank_k5', '_lisa_q_k5', '_lisa_sig_k5', '_lag_k10', '_hetero_k10', '_lag_k15', '_hetero_k15']
gravity_prefix = 'gravity_'

gold_cols = [c for c in cols if not any(s in c for s in spatial_suffixes) and not c.startswith(gravity_prefix)]
spatial_cols = [c for c in cols if any(s in c for s in spatial_suffixes) or c.startswith(gravity_prefix)]

print(f"Total Columns: {len(cols)}")
print(f"Base Metrics (Gold-like): {len(gold_cols)}")
print(f"Spatial/Gravity Features: {len(spatial_cols)}")

# 2. List Expert Variables
expert_vars = [c for c in cols if 'expert_' in c and not any(s in c for s in spatial_suffixes)]
print("\n--- Expert Variables (Base) ---")
for v in sorted(expert_vars):
    print(v)

# 3. Theme Summary
themes = {
    "Expert (Intelligence)": len([c for c in cols if 'expert_' in c]),
    "Spatial (Lag/LISA)": len(spatial_cols),
    "Gravity (Accessibility)": len([c for c in cols if 'gravity_' in c]),
    "Income (Renda)": len([c for c in cols if 'renda' in c or 'rendimento' in c or 'massa_' in c]),
    "Demography (Pop/Age)": len([c for c in cols if 'pop' in c or 'idade' in c or 'demog' in c]),
    "Households (Domicilios)": len([c for c in cols if 'domicilio' in c]),
    "Raw (V00xx)": len([c for c in cols if re.search(r'v\d{4}', c, re.IGNORECASE)])
}

print("\n--- Theme Summary ---")
for t, c in themes.items():
    print(f"{t}: {c}")
