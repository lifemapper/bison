"""Script to run locally or from an EC2 instance to compute PAM statistics from S3 data."""
import datetime as DT
from logging import ERROR
import os
import pandas

from bison.common.log import Logger
from bison.common.aws_util import S3
from bison.common.constants import (
    REGION, S3_BUCKET, S3_OUT_DIR, TMP_PATH, S3_LOG_DIR
)

n = DT.datetime.now()
# underscores for Redshift data
datestr = f"{n.year}_{n.month:02d}_01"
# date for logfile
todaystr = f"{n.year}-{n.month:02d}-{n.day:02d}"

species_county_list_fname = f"county_lists_{datestr}_000.parquet"
diversity_stats_dataname = os.path.join(TMP_PATH, f"diversity_stats_{datestr}.csv")
species_stats_dataname = os.path.join(TMP_PATH, f"species_stats_{datestr}.csv")
site_stats_dataname = os.path.join(TMP_PATH, f"site_stats_{datestr}.csv")


# .............................................................................
def read_species_to_dict(orig_df):
    """Create a dictionary of species keys, with values containing counts for counties.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns:
            census_state, census_county. taxonkey, species, riis_assessment, occ_count

    Returns:
        dictionary of {county: [species: count, species: count, ...]}
    """
    # for each county, create a list of species/count
    county_species_counts = {}
    for _, row in orig_df.iterrows():
        sp = row["species"]
        cty = row["state_county"]
        total = row["occ_count"]
        try:
            county_species_counts[cty].append((sp, total))
        except KeyError:
            county_species_counts[cty] = [(sp, total)]
    return county_species_counts


# .............................................................................
def reframe_to_heatmatrix(orig_df, logger):
    """Create a dataframe of species columns by county rows from county species lists.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns:
            census_state, census_county. taxonkey, species, riis_assessment, occ_count
        logger (object): logger for saving relevant processing messages

    Returns:
        heat_df (Pandas.DataFrame): DF of species (columnns, x axis=1) by counties
            (rows, y axis=0, sites), with values = number of occurrences.
    """
    # Create ST_county column to handle same-named counties in different states
    orig_df["state_county"] = orig_df["census_state"] + "_" + orig_df["census_county"]
    # Create dataframe of zeros with rows=sites and columns=species
    counties = orig_df.state_county.unique()
    species = orig_df.species.unique()
    heat_df = pandas.DataFrame(0, index=counties, columns=species)
    # Fill dataframe
    county_species_counts = read_species_to_dict(orig_df)
    for cty, sp_counts in county_species_counts.items():
        for (sp, count) in sp_counts:
            heat_df.loc[cty][sp] = count
    return heat_df


# .............................................................................
def reframe_to_pam(heat_df, min_val):
    """Create a dataframe of species columns by county rows from county species lists.

    Args:
        heat_df (pandas.DataFrame): DataFrame of species (columnns, x axis=1) by
            counties (rows, y axis=0, sites), with values = number of occurrences.
        min_val(numeric): Minimum value to be considered present/1 in the output matrix.

    Returns:
        pam_df (Pandas.DataFrame): DF of species (columnns, x axis=1) by counties
            (rows, y axis=0, sites), with values = 1 (presence) or 0 (absence).
    """
    try:
        # pandas 2.1.0, upgrade then replace "applymap" with "map"
        pam_df = heat_df.applymap(lambda x: 1 if x >= min_val else 0)
    except AttributeError:
        pam_df = heat_df.applymap(lambda x: 1 if x >= min_val else 0)
    return pam_df


# .............................................................................
class SiteMatrix:
    """Class for managing metric computation for PAM statistics."""

    # ...........................
    def __init__(self, pam_df, logger):
        """Constructor for PAM stats computations.

        Args:
            pam_df (pandas.DataFrame): A presence-absence matrix to use for computations.
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._pam_df = pam_df
        self.logger = logger
        self._report = {}

    # ...............................................
    @property
    def num_species(self):
        """Get the number of species with at least one site present.

        Returns:
            int: The number of species that are present somewhere.

        Note:
            Also used as gamma diversity (species richness over entire landscape)
        """
        count = 0
        if self._pam_df is not None:
            count = int(self._pam_df.any(axis=0).sum())
        return count

    # ...............................................
    @property
    def num_sites(self):
        """Get the number of sites with presences.

        Returns:
            int: The number of sites that have present species.
        """
        count = 0
        if self._pam_df is not None:
            count = int(self._pam_df.any(axis=1).sum())
        return count

    # ...............................................
    def alpha(self):
        """Calculate alpha diversity, the number of species in each site.

        Returns:
            alpha_series (pandas.Series): alpha diversity values for each site.
        """
        alpha_series = None
        if self._pam_df is not None:
            alpha_series = self._pam_df.sum(axis=1)
            alpha_series.name = "alpha_diversity"
        return alpha_series

    # ...............................................
    def alpha_proportional(self):
        """Calculate proportional alpha diversity - percentage of species in each site.

        Returns:
            alpha_pr_series (pandas.Series): column of proportional alpha diversity values for
                each site.
        """
        alpha_pr_series = None
        if self._pam_df is not None:
            alpha_pr_series = self._pam_df.sum(axis=1) / float(self.num_species)
            alpha_pr_series.name = "alpha_proportional_diversity"
        return alpha_pr_series

    # .............................................................................
    def phi(self):
        """Calculate phi, the range size per site.

        Returns:
            phi_series (pandas.Series): column of sum of the range sizes for the species present at each
                site in the PAM.
        """
        phi_series = None
        if self._pam_df is not None:
            phi_series = self._pam_df.dot(self._pam_df.sum(axis=0))
            phi_series.name = "phi_range_sizes"
        return phi_series

    # .............................................................................
    def phi_average_proportional(self):
        """Calculate proportional range size per site.

        Returns:
            phi_avg_pr_series (pandas.Series): column of the proportional value of the
                sum of the range sizes for the species present at each site in the PAM.
        """
        phi_avg_pr_series = None
        if self._pam_df is not None:
            phi_avg_pr_series = self._pam_df.dot(
                self.omega()).astype(float) / (self.num_sites * self.alpha())
            phi_avg_pr_series.name = "phi_average_proportional_range_sizes"
        return phi_avg_pr_series

    # ...............................................
    def beta(self):
        """Calculate beta diversity for each site, Whitaker's ratio: gamma/alpha.

        Returns:
            beta_series (pandas.Series): ratio of gamma to alpha for each site.

        TODO: revisit this definition, also consider beta diversity region compared
            to region
        """
        import numpy
        beta_series = None
        if self._pam_df is not None:
            beta_series = float(self.num_species) / self._pam_df.sum(axis=1)
            beta_series.replace([numpy.inf, -numpy.inf], 0, inplace=True)
            beta_series.name = "whittakers_gamma/alpha_ratio_beta_diversity"
        return beta_series

    # ...............................................
    def omega(self):
        """Calculate the range size (number of counties) per species.

        Returns:
            omega_series (pandas.Series): A row of range sizes for each species.
        """
        omega_series = None
        if self._pam_df is not None:
            omega_series = self._pam_df.sum(axis=0)
            omega_series.name = "omega"
        return omega_series

    # ...............................................
    def omega_proportional(self):
        """Calculate the mean proportional range size of each species.

        Returns:
            beta_series (pandas.Series): A row of the proportional range sizes for
                each species.
        """
        omega_pr_series = None
        if self._pam_df is not None:
            omega_pr_series = self._pam_df.sum(axis=0) / float(self.num_sites)
            omega_pr_series.name = "omega_proportional"
        return omega_pr_series

    # .............................................................................
    def psi(self):
        """Calculate the range richness of each species.

        Returns:
            psi_df (pandas.Series): A Series of range richness for the sites that
                each species is present in.

        TODO: revisit this
        """
        psi_series = None
        if self._pam_df is not None:
            psi_series = self._pam_df.sum(axis=1).dot(self._pam_df)
            psi_series.name = "psi"
        return psi_series

    # .............................................................................
    def psi_average_proportional(self):
        """Calculate the mean proportional range richness.

        Returns:
            psi_avg_df (pandas.DataFrame): A Series of proportional range richness
                for the sites that each species in the PAM is present.

        TODO: revisit this
        """
        psi_avg_series = None
        if self._pam_df is not None:
            psi_avg_series = (
                    self.alpha().dot(self._pam_df).astype(float)
                    / (self.num_species * self.omega())
            )
            psi_avg_series.name = "psi_average_proportional"
        return psi_avg_series

    # ...............................................
    def whittaker(self):
        """Calculate Whittaker's beta diversity metric for a PAM.

        Returns:
            whittaker_dict: Whittaker's beta diversity for the PAM.
        """
        whittaker_dict = {}
        if self._pam_df is not None:
            whittaker_dict["whittaker_beta_diversity"] = float(
                self.num_species / self.omega_proportional().sum())
        return whittaker_dict

    # ...............................................
    def lande(self):
        """Calculate Lande's beta diversity metric for a PAM.

        Returns:
            lande_dict: Lande's beta diversity for the PAM.
        """
        lande_dict = {}
        if self._pam_df is not None:
            lande_dict["lande_beta_diversity"] = float(
                self.num_species -
                (self._pam_df.sum(axis=0).astype(float) / self.num_sites).sum()
            )
        return lande_dict

    # ...............................................
    def legendre(self):
        """Calculate Legendre's beta diversity metric for a PAM.

        Returns:
            legendre_dict: Legendre's beta diversity for the PAM.
        """
        legendre_dict = {}
        if self._pam_df is not None:
            legendre_dict["legendre_beta_diversity"] = float(
                self.omega().sum() -
                (float((self.omega() ** 2).sum()) / self.num_sites)
            )
        return legendre_dict

    # ...............................................
    def calculate_diversity_statistics(self):
        """Compute PAM diversity statistics.

        Returns:
            diversity_df (pandas.DataFrame): A matrix of values with columns as
                diversity metric names, and one row with values.
        """
        diversity_df = None
        if self._pam_df is not None:
            # Merge dictionaries using unpack operator d5 = {**d1, **d2}
            diversity_stats = {
                **self.lande(), **self.legendre(), **self.whittaker(),
                **{"num_sites": self.num_sites}, **{"num_species": self.num_species}
            }
            diversity_df = pandas.DataFrame(diversity_stats, index=["value"])
        return diversity_df

    # ...............................................
    def calculate_site_statistics(self):
        """Calculate site-based statistics.

        Returns:
            site_stats_matrix(pandas.DataFrame): A matrix of site-based statistics for
                the selected metrics.  Columns are statistic names, rows are sites
                (counties).
        """
        site_stats_df = None
        if self._pam_df is not None:
            site_stats = [
                self.alpha(), self.alpha_proportional(), self.beta(), self.phi(),
                self.phi_average_proportional()]
            # Create matrix with each series as a row, columns = sites, rows = stats
            site_stats_df = pandas.DataFrame(site_stats)
            # Transpose to put statistics in columns, sites/counties in rows
            site_stats_df = site_stats_df.T
        return site_stats_df

    # ...............................................
    def calculate_species_statistics(self):
        """Calculate site-based statistics.

        Returns:
            species_stats_df (pandas.DataFrame): A matrix of species-based statistics
                for the selected metrics.  Columns are statistics, rows are species.
        """
        species_stats_df = None
        if self._pam_df is not None:
            species_stats = [
                self.omega(), self.omega_proportional(), self.psi(),
                self.psi_average_proportional()
            ]
            # Create matrix with each series as a row, columns = species, rows = stats
            species_stats_df = pandas.DataFrame(species_stats)
            # Transpose to put columns = stats, rows = species
            species_stats_df = species_stats_df.T
        return species_stats_df

    # ...............................................
    def write_to_csv(self, filename):
        """Write dataframe as CSV format, with comma separator, utf-8 format.

        Args:
            filename: full path to output file.
        """
        try:
            self._pam_df.to_csv(filename)
        except Exception as e:
            self.logger.log(f"Failed to write {filename}: {e}", log_level=ERROR)


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger = Logger(script_name)

    s3 = S3(region=REGION)

    # Read directly into DataFrame
    orig_df = s3.get_dataframe_from_parquet(
        S3_BUCKET, S3_OUT_DIR, species_county_list_fname)

    heat_df = reframe_to_heatmatrix(orig_df, logger)
    pam_df = reframe_to_pam(heat_df, 1)
    pam = SiteMatrix(pam_df, logger)

    diversity_df = pam.calculate_diversity_statistics()
    site_stat_df = pam.calculate_site_statistics()
    species_stat_df = pam.calculate_species_statistics()

    diversity_stats_dataname = os.path.join(TMP_PATH, f"diversity_stats_{datestr}.csv")
    species_stats_dataname = os.path.join(TMP_PATH, f"species_stats_{datestr}.csv")
    site_stats_dataname = os.path.join(TMP_PATH, f"site_stats_{datestr}.csv")

    diversity_df.to_csv(diversity_stats_dataname)
    site_stat_df.to_csv(site_stats_dataname)
    species_stat_df.to_csv(species_stats_dataname)

    # Write CSV and Parquet versions to S3
    s3_outpath = f"s3://{S3_BUCKET}/{S3_OUT_DIR}"
    diversity_df.to_csv(f"{s3_outpath}/diversity_stats_{datestr}.csv")
    site_stat_df.to_csv(f"{s3_outpath}/site_stats_{datestr}.csv")
    species_stat_df.to_csv(f"{s3_outpath}/species_stats_{datestr}.csv")
    diversity_df.to_parquet(f"{s3_outpath}/diversity_stats_{datestr}.parquet")
    site_stat_df.to_parquet(f"{s3_outpath}/site_stats_{datestr}.parquet")
    species_stat_df.to_parquet(f"{s3_outpath}/species_stats_{datestr}.parquet")

    # Upload logfile to S3
    uploaded_s3name = s3.upload(logger.filename, S3_BUCKET, S3_LOG_DIR)
