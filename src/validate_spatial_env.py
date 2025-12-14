import duckdb
import sys
import importlib.util

def check_package(name):
    if name in sys.modules:
        print(f"✅ {name} already imported")
        return True
    elif (spec := importlib.util.find_spec(name)) is not None:
        print(f"✅ {name} found")
        return True
    else:
        print(f"❌ {name} NOT found")
        return False

print("--- Validating Spatial Environment ---")

# 1. Check Python Libraries
packages = ['geopandas', 'shapely', 'geobr', 'libpysal']
missing = []
for pkg in packages:
    if not check_package(pkg):
        missing.append(pkg)

# 2. Check DuckDB Spatial Extension
print("\n--- Checking DuckDB Spatial Extension ---")
try:
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    print("✅ DuckDB 'spatial' extension installed and loaded successfully.")
    
    # Test a simple spatial query
    result = con.execute("SELECT ST_Point(0, 0) as point").fetchone()
    print(f"✅ Spatial query test: {result}")
    
except Exception as e:
    print(f"❌ DuckDB Spatial Error: {e}")

if missing:
    print(f"\n⚠️ Missing Python packages: {', '.join(missing)}")
    print("Run: pip install " + " ".join(missing))
else:
    print("\n🎉 Environment looks ready for Spatial Intelligence!")
