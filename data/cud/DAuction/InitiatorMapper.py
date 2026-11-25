import pandas as pd

class InitiatorMapper:
    def __init__(self, mappings_path):
        self.initiator_map_df = pd.read_excel(mappings_path, index_col=0)

    def _is_empty_or_none(self, s):
        return not s

    def _get_mapped_col(self, initiator, col_to_map):
        sub_df = self.initiator_map_df.loc[
            self.initiator_map_df["IncomingInitiator"] == initiator
        ]
        if len(sub_df) == 0:
            res = ""
        else:
            res = sub_df[col_to_map].iloc[0]

        if not self._is_empty_or_none(res):
            return str(res)
        else:
            return ""

    def get_incomingis_hastener_servicer(self, initiator):
        return self._get_mapped_col(initiator, col_to_map="IncomingIs_Hastener_Servicer")

    def get_incoming_hastener(self, initiator):
        return self._get_mapped_col(initiator, col_to_map="IncomingHastener")

    def get_incoming_initiator_cepal_ab(self, initiator):
        return self._get_mapped_col(initiator, col_to_map="IncomingInitiator_Cepal_AB")

    def get_incoming_portfolio_category(self, initiator):
        return self._get_mapped_col(initiator, col_to_map="IncomingPortfolio_category")

# Usage example:
# Assuming 'initiators_mappings_path' is defined with the path to your Excel file
# mapper = InitiatorMapper(initiators_mappings_path)
# result = mapper.get_incoming_initiator_cepal_ab('SomeInitiator')
