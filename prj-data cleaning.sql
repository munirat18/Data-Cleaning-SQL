-- Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardise the Data
-- 3. Null Values or blank values
-- 4. Remove Any blank Columns or Rows


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;


SELECT * , row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(SELECT * , row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
as row_num FROM layoffs_staging)

select * from duplicate_cte where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT * , row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
as row_num FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;

-- Standardising Data

select company, trim(company) from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry from layoffs_staging2
order by 1;

select * from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct location from layoffs_staging2
order by 1;

select distinct country from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country) from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y') from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date` from layoffs_staging2
order by 1;

alter table layoffs_staging2
modify column `date` date;

SELECT * FROM layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select * from layoffs_staging2
where industry is null or industry = '';

update layoffs_staging2
set industry = null 
where industry = '';

select * from layoffs_staging2
where company = 'Airbnb';

select * from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * from layoffs_staging2;

SELECT * FROM layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

DELETE FROM layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select * from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;