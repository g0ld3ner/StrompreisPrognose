from datetime import datetime
from typing import List, Optional

from sqlalchemy import Float, Integer, String, DateTime, ForeignKey, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Location(Base):
    __tablename__ = "locations"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    state: Mapped[str] = mapped_column(String(50), nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    
    # Establish a one-to-many relationship with WeatherHistory
    weather_history: Mapped[List["WeatherHistory"]] = relationship(
        "WeatherHistory", back_populates="location", cascade="all, delete-orphan"
    )

    # Composite unique constraint to ensure we don't have duplicate coordinates
    __table_args__ = (
        Index("idx_location_lat_lon", "latitude", "longitude", unique=True),
    )

    def __repr__(self):
        return f"<Location(id={self.id}, state='{self.state}', lat={self.latitude}, lon={self.longitude})>"

class WeatherHistory(Base):
    __tablename__ = "weather_history"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    
    # Weather metrics
    temperature_2m: Mapped[Optional[float]] = mapped_column(Float)
    wind_speed_100m: Mapped[Optional[float]] = mapped_column(Float)
    sunshine_duration: Mapped[Optional[float]] = mapped_column(Float)
    global_tilted_irradiance: Mapped[Optional[float]] = mapped_column(Float)
    
    location: Mapped["Location"] = relationship("Location", back_populates="weather_history")
    
    # Composite unique constraint: One weather record per location per hour
    __table_args__ = (
        Index("idx_weather_location_time", "location_id", "timestamp", unique=True),
    )

    def __repr__(self):
        return f"<WeatherHistory(loc_id={self.location_id}, time='{self.timestamp}')>"

class ElectricityPrice(Base):
    __tablename__ = "electricity_prices"
    
    # Using timestamp as primary key since there's exactly one price per hour for Germany
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)  # in EUR/MWh

    def __repr__(self):
        return f"<ElectricityPrice(time='{self.timestamp}', price={self.price})>"

# Optional: Weather Forecast table if we want to store predictions separately
class WeatherForecast(Base):
    __tablename__ = "weather_forecast"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id"), nullable=False)
    
    # The time the forecast was generated (important for historical tracking)
    forecast_generated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    # The time the forecast is for
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    
    temperature_2m: Mapped[Optional[float]] = mapped_column(Float)
    wind_speed_100m: Mapped[Optional[float]] = mapped_column(Float)
    sunshine_duration: Mapped[Optional[float]] = mapped_column(Float)
    global_tilted_irradiance: Mapped[Optional[float]] = mapped_column(Float)
    
    __table_args__ = (
        Index("idx_forecast_loc_gen_time", "location_id", "forecast_generated_at", "timestamp", unique=True),
    )
