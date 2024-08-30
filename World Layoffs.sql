CREATE TABLE `layoffs_staging2` (
  `company` varchar(50) DEFAULT NULL,
  `location` varchar(50) DEFAULT NULL,
  `industry` varchar(50) DEFAULT NULL,
  `total_laid_off` varchar(50) DEFAULT NULL,
  `percentage_laid_off` varchar(50) DEFAULT NULL,
  `date` varchar(50) DEFAULT NULL,
  `stage` varchar(50) DEFAULT NULL,
  `country` varchar(50) DEFAULT NULL,
  `funds_raised_millions` varchar(50) DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs;


INSERT INTO `layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;







-- 2. Standardize Data

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM world_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y', '%Y-%m-%d')
WHERE `date` LIKE '%m/%d/%Y';

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT `date`
FROM layoffs_staging2
ORDER BY 1;

DELETE FROM layoffs_staging2
WHERE `date` IS NULL;





-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

USE sqlportofolio;

SELECT * 
FROM layoffs_staging2;

SELECT * FROM layoffs;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;



-- Query Project EDA

WITH layoff_metrics AS (
    SELECT 
        industry,
        country,
        YEAR(STR_TO_DATE(`date`, '%m/%d/%Y')) AS layoff_year,
        QUARTER(STR_TO_DATE(`date`, '%m/%d/%Y')) AS layoff_quarter,
        SUM(CAST(REPLACE(total_laid_off, ',', '') AS UNSIGNED)) AS total_layoffs,
        AVG(CAST(REPLACE(percentage_laid_off, '%', '') AS DECIMAL(5,2))) AS avg_percentage_laid_off,
        COUNT(DISTINCT company) AS affected_companies,
        SUM(CAST(REPLACE(funds_raised_millions, '$', '') AS DECIMAL(10,2))) AS total_funds_raised
    FROM layoffs_staging2
    WHERE industry != '' 
      AND country != '' 
      AND total_laid_off != '' 
      AND percentage_laid_off != '' 
      AND `date` != '' 
      AND funds_raised_millions != ''
    GROUP BY industry, country, layoff_year, layoff_quarter
),
industry_rankings AS (
    SELECT 
        industry,
        layoff_year,
        SUM(total_layoffs) AS yearly_layoffs,
        ROW_NUMBER() OVER (PARTITION BY layoff_year ORDER BY SUM(total_layoffs) DESC) AS industry_rank
    FROM layoff_metrics
    GROUP BY industry, layoff_year
),
country_rankings AS (
    SELECT 
        country,
        layoff_year,
        SUM(total_layoffs) AS yearly_layoffs,
        ROW_NUMBER() OVER (PARTITION BY layoff_year ORDER BY SUM(total_layoffs) DESC) AS country_rank
    FROM layoff_metrics
    GROUP BY country, layoff_year
)

SELECT 
    lm.industry,
    lm.country,
    lm.layoff_year,
    lm.layoff_quarter,
    lm.total_layoffs,
    lm.avg_percentage_laid_off,
    lm.affected_companies,
    lm.total_funds_raised,
    ir.yearly_layoffs AS industry_yearly_layoffs,
    ir.industry_rank,
    cr.yearly_layoffs AS country_yearly_layoffs,
    cr.country_rank,
    (lm.total_layoffs / lm.affected_companies) AS avg_layoffs_per_company,
    (lm.total_funds_raised / lm.total_layoffs) AS funds_raised_per_layoff
FROM layoff_metrics lm
JOIN industry_rankings ir ON lm.industry = ir.industry AND lm.layoff_year = ir.layoff_year
JOIN country_rankings cr ON lm.country = cr.country AND lm.layoff_year = cr.layoff_year
ORDER BY lm.layoff_year DESC, lm.layoff_quarter DESC, lm.total_layoffs DESC
LIMIT 1000;