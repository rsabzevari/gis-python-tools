--SELECT * FROM zone_review.state_cadastre;

-- 1--sreate spatial index on geometry column
CREATE INDEX cadastre_geom_gix
ON zone_review.state_cadastre
USING gist (geom);

CREATE INDEX zone_geom_gix
ON zone_review.state_zone
USING gist(geom);

--2--create field 'cad_area'
ALTER TABLE zone_review.state_cadastre
ADD COLUMN IF NOT EXISTS cad_area double precision;


UPDATE zone_review.state_cadastre
SET cad_area = ST_Area(geom);

--3--create intersected table
CREATE TABLE zone_review.intersected AS
SELECT
	c.cadid,
	c.cad_area,
	z."LAY_CLASS",
	z."SYM_CODE",
	ST_Intersection(c.geom, z.geom) AS geom -- renaming the output column (new geometry) -- this creates a new geometry (the overlap).
FROM zone_review.state_cadastre AS c
JOIN zone_review.state_zone AS z
	ON ST_Intersects(c.geom, z.geom)

	
-- SELECT * FROM zone_review.intersected
-- SELECT * FROM zone_review.zone_counts
-- SELECT * FROM zone_review.multi_zone_lots
--SELECT * FROM zone_review.slices


--4--detecting each cadid has how many zones in intersected table
CREATE TABLE zone_review.zone_counts AS 
SELECT
	cadid,
	COUNT(DISTINCT "LAY_CLASS") AS n_zones
FROM zone_review.intersected
GROUP BY cadid


--5--filter cadid s with more than one zones
CREATE TABLE zone_review.multi_zone_lots AS 
SELECT
	cadid,
	n_zones
FROM zone_review.zone_counts
WHERE n_zones > 1;

--Step 4 and 5 can be done in one step using HAVING statement

--6--oin filtered cadid s into intersected layer (to get only the slices with more than one zoning)
CREATE TABLE zone_review.slices AS 
SELECT 
	i.cadid,
	i.cad_area,
	i."LAY_CLASS",
	i."SYM_CODE",
	i.geom
FROM zone_review.intersected as i
JOIN zone_review.multi_zone_lots as m
	ON (i.cadid = m.cadid)


--7--calculate coverage of each slice on every lot
ALTER TABLE zone_review.slices
	ADD COLUMN slice_area double precision,
	ADD COLUMN coverage double precision

UPDATE zone_review.slices
	SET slice_area = ST_Area(geom)
	SET coverage = slice_area/ cad_area * 100



--8-- Drop all tables we created and don't need
DROP TABLE IF EXISTS zone_review.intersected;
DROP TABLE IF EXISTS zone_review.zone_counts;
DROP TABLE IF EXISTS zone_review.multi_zone_lots;
		

	
