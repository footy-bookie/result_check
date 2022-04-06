import os

import google.auth
import pandas as pd

from helper import read_bigquery, write_storage, write_bq


class ResultCheck:
    def __init__(self):
        self.results = read_bigquery("footy_data_warehouse", "src_matches_import").iloc[::-1].reset_index()

        self.predicted_results = read_bigquery('footy_data_warehouse', 'total_prediction').sort_values(
            'home_team_name').reset_index(
            drop=True)

        self.credentials, self.project_id = google.auth.default()

    def actual_results(self):
        df = self.results
        df.rename(columns={"index": "match_id"}, inplace=True)

        df.drop(['stadium_name', 'referee', 'attendance'], axis=1, inplace=True)
        df.iloc[:, 6:] = df.iloc[:, 6:][df.iloc[:, 6:].columns].apply(pd.to_numeric, errors='coerce')

        df_last_completed = df[df['status'] == 'complete'].head(9).sort_values('home_team_name')
        df_last_completed['goal_diff'] = df_last_completed['home_team_goal_count'] - df_last_completed[
            'away_team_goal_count']

        for index, row in df_last_completed[df_last_completed['status'] == 'complete'].iterrows():
            if df_last_completed['goal_diff'][index] > 0:
                df_last_completed.at[index, 'real_result'] = 3
            elif df_last_completed['goal_diff'][index] == 0:
                df_last_completed.at[index, 'real_result'] = 2
            else:
                df_last_completed.at[index, 'real_result'] = 1
        return df_last_completed.reset_index(drop=True)

    def possible_win(self):
        df = self.actual_results()
        df_predicted = self.predicted_results

        if df['match_id'].isin(df_predicted['match_id']).sum() == 8:
            df['possible_win'] = 0
            for index, row in df.iterrows():
                if df['real_result'][index] == df_predicted.copy()['predicted_result'][index]:
                    if df['real_result'][index] == 3:
                        df.at[index, 'possible_win'] = df['odds_ft_home_team_win'][index] * 10
                    elif df['real_result'][index] == 0:
                        df.at[index, 'possible_win'] = df['odds_ft_draw'][index] * 10
                    else:
                        df.at[index, 'possible_win'] = df['odds_ft_away_team_win'][index] * 10

            df['predicted_results'] = df_predicted.copy()['predicted_result']
            df_short = df[['match_id', 'date_GMT', 'status', 'home_team_name', 'away_team_name', 'real_result',
                           'possible_win', 'predicted_results']]
            print(df_short)
            write_bq(df_short, self.project_id, 'footy_data_warehouse', os.getenv('TOTAL_RESULT_CHECK_BQ'),
                     self.credentials)
            write_storage(df_short, os.getenv('RESULT_CHECK_OVER_TIME_SINK'))

            return df
        else:
            print("Match day is not over yet!")
