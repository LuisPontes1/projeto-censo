import pandas as pd
df = pd.read_parquet(r"c:\projetos\projeto-censo\data\diamond\censo_2022_diamond_features_final.parquet")
print([c for c in df.columns if 'distrito' in c or 'bairro' in c or 'subdistrito' in c])
