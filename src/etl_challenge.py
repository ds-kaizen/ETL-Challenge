# importing necessary packages
from os.path import basename
import pandas as pd
import numpy as np
import glob
import logging

import yaml

with open(
    "src/config.yaml", "r"
) as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

# import config parameter
basepath = config["basepath"]
inbound = config["inbound"]
outbound = config["outbound"]
log = config["log"]
valid_col_ls = config["valid_col_ls"]
col_rename = config["col_rename"]
col_seq = config["col_seq"]


class epl_league:
    def __init__(self, file_list):
        self.file_list = file_list

    def read_input_json_file(self, file_list):
        """this method will read all the input json file from input 'data' folder"""

        epl_df = []
        for f in file_list:
            # extracting season information from input file name
            season = basename(f).removesuffix(".json")
            json = pd.read_json(f)
            json["Season"] = season
            epl_df.append(json)
        epl_df = pd.concat(epl_df)
        # reset index to avoid duplicate index operation
        epl_df = epl_df.reset_index()

        return epl_df

    def drop_bad_cols(self, epl_df):
        """this method will drop all the columns which is not required for job processing"""

        epl_df = epl_df[valid_col_ls]

        return epl_df

    def rename_cols(self, epl_df):
        """this method will renaming columns to make it more readable"""

        epl_df.rename(columns=col_rename, inplace=True)

        return epl_df

    def points(self, epl_df):
        """ this method will add columns which will keep track of points won or lost by a team"""

        for i, recs in epl_df.iterrows():
            if recs["FullTimeResult"] == "H":
                epl_df.loc[i, "home_wins"] = 1
                epl_df.loc[i, "home_points"] = 3
            if recs["FullTimeResult"] == "A":
                epl_df.loc[i, "away_wins"] = 1
                epl_df.loc[i, "away_points"] = 3
            if recs["FullTimeResult"] == "A":
                epl_df.loc[i, "home_loss"] = 1
            if recs["FullTimeResult"] == "H":
                epl_df.loc[i, "away_loss"] = 1
            if recs["FullTimeResult"] == "D":
                epl_df.loc[i, "draw"] = 1

        return epl_df

    def home_team(self, epl_df):
        """this method will create dataframe for home team"""

        groupby_cols = ["HomeTeam", "Season", "LeagueDivision"]

        epl_df_home = (
            epl_df.groupby(groupby_cols)
            .agg(
                {
                    "home_wins": "count",
                    "home_loss": "count",
                    "home_points": "sum",
                    "draw": "count",
                    "FullTimeResult": "count",
                    "FullTimeHomeTeamGoals": "sum",
                    "FullTimeAwayTeamGoals": "sum",
                }
            )
            .reset_index()
            .rename(
                columns={
                    "FullTimeResult": "total_home_matches",
                    "FullTimeHomeTeamGoals": "goals_for_home",
                    "FullTimeAwayTeamGoals": "goals_against_home",
                }
            )
        )

        return epl_df_home

    def away_team(self, epl_df):
        """this method will create dataframe for away team"""

        groupby_cols = ["AwayTeam", "Season", "LeagueDivision"]

        epl_df_away = (
            epl_df.groupby(groupby_cols)
            .agg(
                {
                    "away_wins": "count",
                    "away_loss": "count",
                    "away_points": "sum",
                    "draw": "count",
                    "FullTimeResult": "count",
                    "FullTimeAwayTeamGoals": "sum",
                    "FullTimeHomeTeamGoals": "sum",
                }
            )
            .reset_index()
            .rename(
                columns={
                    "FullTimeResult": "total_away_matches",
                    "FullTimeAwayTeamGoals": "goals_for_away",
                    "FullTimeHomeTeamGoals": "goals_against_away",
                }
            )
        )

        return epl_df_away

    def league_table(self, epl_df_home, epl_df_away):
        """this method will concat home_team dataframe with away_team dataframe to create EPL position table"""

        # to concat away_team and home_team, first we need to rename columns.
        col_rename_home = {
            "HomeTeam": "club",
            "total_home_matches": "matches_played",
            "home_wins": "wins",
            "home_loss": "loss",
            "goals_for_home": "goals_for",
            "goals_against_home": "goals_against",
            "home_points": "points",
            "Season": "season",
        }

        epl_df_home = epl_df_home.rename(columns=col_rename_home)

        col_rename_away = {
            "AwayTeam": "club",
            "total_away_matches": "matches_played",
            "away_wins": "wins",
            "away_loss": "loss",
            "goals_for_away": "goals_for",
            "goals_against_away": "goals_against",
            "away_points": "points",
            "Season": "season",
        }

        epl_df_away = epl_df_away.rename(columns=col_rename_away)

        league_table = pd.concat([epl_df_home, epl_df_away], axis=0)

        # grouping league table recs to produce desired output
        groupby_cols = ["club", "LeagueDivision", "season"]

        league_table = (
            league_table.groupby(groupby_cols)
            .agg(
                {
                    "matches_played": "sum",
                    "wins": "sum",
                    "loss": "sum",
                    "draw": "sum",
                    "goals_for": "sum",
                    "goals_against": "sum",
                    "points": "sum",
                }
            )
            .reset_index()
        )

        # adding draw points to 'points' column
        league_table["points"] = league_table["draw"].astype(int) + league_table[
            "points"
        ].astype(int)

        # calculating goal difference
        league_table["goal_difference"] = (
            league_table["goals_for"] - league_table["goals_against"]
        )

        # drop column 'LeagueDivision'
        league_table = league_table.drop("LeagueDivision", axis=1)

        # sorting league table
        league_table["goal_difference_abs"] = league_table["goal_difference"].abs()
        league_table["goal_difference_le"] = league_table["goal_difference"].le(0)
        league_table = league_table.sort_values(
            ["season", "points", "goal_difference_le", "goal_difference_abs"],
            ascending=[False, False, True, True],
        ).drop(["goal_difference_le", "goal_difference_abs"], axis=1)

        # arranging column sequence
        league_table = league_table[col_seq]

        return league_table

    def best_scoring_team_by_season(self, league_table):
        """this method will create dataframe for best scoring team by season"""

        champion_table = league_table.loc[
            league_table.groupby(["season"])["points"].idxmax()
        ].sort_values(["season"], ascending=False)

        return champion_table

    def write_excel(self, league_table, champion_table):
        """this methos will write output into excel file"""

        writer_league = pd.ExcelWriter(
            basepath + outbound + "/EPL_League.xlsx",
            mode="w",
            engine="xlsxwriter",
            date_format="m/d/yyyy",
            datetime_format="m/d/yyyy",
        )
        writer_champ = pd.ExcelWriter(
            basepath + outbound + "/Best_Scoring_Team.xlsx",
            mode="w",
            engine="xlsxwriter",
            date_format="m/d/yyyy",
            datetime_format="m/d/yyyy",
        )

        row = 0
        sheet_league = "league_table"
        sheet_champ = "top_team_by_season"

        league_table.to_excel(
            writer_league, sheet_name=sheet_league, index=False, startrow=row
        )

        champion_table.to_excel(
            writer_champ, sheet_name=sheet_champ, index=False, startrow=row
        )

        writer_league.save()
        writer_champ.save()

    def main(self):
        """this method will be the starting point of ETL process"""
        try:
            epl_df = self.read_input_json_file(file_list)

            epl_df = self.drop_bad_cols(epl_df)

            epl_df = self.rename_cols(epl_df)

            epl_df = self.points(epl_df)

            epl_df_home = self.home_team(epl_df)

            epl_df_away = self.away_team(epl_df)

            league_table = self.league_table(epl_df_home, epl_df_away)

            champion_table = self.best_scoring_team_by_season(league_table)

            self.write_excel(league_table, champion_table)

        except Exception as e:
            logging.error("Exception occured", exc_info=True)
            logger.info(f"ETL Process Failed, fix the issue and rerun!! \n")


def logs():

    # create log filename
    filenm = basepath + log + "/" + "epl_league" + ".log"

    logging.basicConfig(
        filename=filenm,
        format="%(levelname)s - %(asctime)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
        filemode="w",
    )

    # Creating an object
    logger = logging.getLogger()

    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    return logger


if __name__ == "__main__":

    filepath = basepath + inbound + "/season*.json"
    file_list = glob.glob(filepath)

    # initialising logger
    logger = logs()
    logger.info(
        "*************************EPL ETL Challenge program started************************* \n"
    )

    p = epl_league(file_list)
    p.main()

    logger.info(
        "*************************EPL ETL Challenge program completed successfully************************* \n"
    )
