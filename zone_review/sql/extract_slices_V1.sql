-- Multi-zone lots analysis
-- Input:
--   zone_review.state_cadastre  : lot polygons (geom)
--   zone_review.state_zone      : zoning polygons (geom)
-- Output:
--   zone_review.multi_zone_slices : per-lot slices for multi-zoned lots,
--   with slice area and % coverage of the lot

-- 1. Spatial indexes to speed up ST_Intersects / ST_Intersection
CREATE INDEX cadastre_geom_gix
	ON zone_review.state_cadastre
	USING gist (geom);

CREATE INDEX zone_geom_gix
	ON zone_review.state_zone
	USING gist(geom);

-- 2. Build analysis table
CREATE TABLE zone_review.multi_zone_slices AS 
WITH intersected AS (
		-- Intersect lots and zones → one row per (lot, zone) slice
		SELECT 
			c.cadid,
			ST_Area(c.geom) AS cad_area, 
			z."LAY_CLASS",
			z."SYM_CODE",
			ST_Intersection(c.geom, z.geom) as geom
		FROM zone_review.state_cadastre as c
		JOIN zone_review.state_zone as z
			ON ST_Intersects(c.geom, z.geom)
	),
	multi_zones AS (
		-- Keep only lots that have more than 1 distinct zoning class
		SELECT
			cadid,
			COUNT(DISTINCT "LAY_CLASS") as n_zones
		FROM intersected
		GROUP BY cadid
		HAVING COUNT(DISTINCT "LAY_CLASS") > 1
		),
		slices AS (
			SELECT
				i.cadid,
				i.cad_area,
				i."LAY_CLASS",
				i."SYM_CODE",
				i.geom
			FROM intersected as i
			JOIN multi_zones as m
				ON (i.cadid = m.cadid)
	),
	slice_area AS (
		-- Compute slice_area once so we can reuse it for coverage
		SELECT 
			cadid,
			cad_area,
			"LAY_CLASS",
			"SYM_CODE",
			ST_Area(geom) AS slice_area,
			geom
		FROM slices		
	)			
SELECT 
	cadid,
	cad_area,
	"LAY_CLASS",
	"SYM_CODE",
	slice_area,
	slice_area / cad_area * 100 AS coverage,
	geom
FROM slice_area;

-- 3. Spatial index on result for fast viewing / queries		
CREATE INDEX multi_zone_slices_geom_gix
  ON zone_review.multi_zone_slices
  USING gist (geom);

	