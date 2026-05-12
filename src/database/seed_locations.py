import sys
import os

# Add the old reference project to the python path to import the locations dictionary
sys.path.append(os.path.join(os.path.dirname(__file__), "../../_reference_old_project/src"))


# pyrefly: ignore [missing-import]
from locations import location
# pyrefly: ignore [missing-import]
from src.database.database import SessionLocal, engine
# pyrefly: ignore [missing-import]
from src.database.models import Base, Location

def seed_locations():
    print("Initializing Database schema...")
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    print("Seeding locations...")
    db = SessionLocal()
    
    added_count = 0
    skipped_count = 0
    
    try:
        # Loop over the dictionary: state -> list of (lat, lon) tuples
        for state, coords in location.items():
            for lat, lon in coords:
                # Check if this location already exists
                existing = db.query(Location).filter_by(latitude=lat, longitude=lon).first()
                if not existing:
                    new_loc = Location(state=state, latitude=lat, longitude=lon)
                    db.add(new_loc)
                    added_count += 1
                else:
                    skipped_count += 1
                    
        db.commit()
        print(f"Done! Added {added_count} new locations. Skipped {skipped_count} existing.")
    except Exception as e:
        db.rollback()
        print(f"Error during seeding: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    seed_locations()
