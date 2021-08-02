import csv
import utility_func as uf
from bank_txn import BankTxn
from treasury_txn import TreasuryTxn
from bank_acct import BankAcct
from match import Match
from match_state_summary import MatchStateSummary
from matchmaker import MatchMaker
from matchmaker_runner import MatchmakerRunner
from key_matchmaker import KeyMatchmaker


def process_acct(bank_acct:BankAcct, in_folder, out_folder):
    partner = bank_acct.account_name
    gl_number = bank_acct.netsuite_account_number
    print(f"account {bank_acct.account_id} (gl_number = {gl_number})")
    unmatched_tr = uf.get_lst_obj(f'data/{in_folder}/all_tr0.csv', TreasuryTxn, uf.obj_is_acct(bank_acct.account_id))
    unmatched_br = uf.get_lst_obj(f'data/{in_folder}/all_br0.csv', BankTxn, uf.obj_is_acct(bank_acct.account_id))
    if len(unmatched_tr) == 0 and len(unmatched_br) == 0:
        return [partner, bank_acct.account_id, gl_number, 0, 0, 0, 0]
    uf.create_folder_if_needed(f"data/{out_folder}/{partner}")
    matchmaker_runner = MatchmakerRunner(set(unmatched_tr), set(unmatched_br))
    matchmaker_runner.set_matchmakers_lst(bank_acct.matchmaker_lst)
    matchmaker_runner.run_all_matchmakers()
    summary = matchmaker_runner.match_state.output_to_files(f"data/{out_folder}/unreconciled_tr.csv",
                                                  f"data/{out_folder}/unreconciled_br.csv",
                                                  f"data/{out_folder}/{partner}/reconciled_{gl_number}.csv",
                                                  TreasuryTxn.sort_by_date,
                                                  BankTxn.sort_by_date
                                                )
    return [partner, bank_acct.account_id, gl_number, len(unmatched_tr), (len(unmatched_tr) - summary.unmatched_a_count),
            len(unmatched_br), (len(unmatched_br) - summary.unmatched_b_count)]

# def record_data_errors(in_folder, out_folder):
#     uf.save_obj_errors(f'data/{in_folder}/all_tr0.csv', TreasuryTxn, 'data/recons_agg/error_catching_treasury_txn.csv')
#     uf.save_obj_errors(f'data/{in_folder}/all_br0.csv', BankTxn, 'data/recons_agg/error_catching_bank_txn.csv')
#     uf.save_obj_errors(f'data/{in_folder}/active_bank_accounts_07', BankAcct, 'data/recons_agg/error_catching_bank_accts.csv')

def record_data_errors(in_folder, out_folder):
    uf.save_obj_errors(f'data/{in_folder}/all_tr0.csv', TreasuryTxn, 'data/recons_event/error_catching_treasury_txn.csv')
    uf.save_obj_errors(f'data/{in_folder}/all_br0.csv', BankTxn, 'data/recons_event/error_catching_bank_txn.csv')
    uf.save_obj_errors(f'data/{in_folder}/active_bank_accounts_07', BankAcct, 'data/recons_event/error_catching_bank_accts.csv')

def main():
    # in_folder = 'agg'
    # out_folder = 'recons_agg'
    # record_data_errors(out_folder)
    # ares = uf.get_lst_obj('data/active_bank_accounts_07', BankAcct)
    # with open(f'data/{out_folder}/matching_summary.csv', "w", newline='') as csv_file:
    #     csv_writer = csv.writer(csv_file)
    #     csv_writer.writerow(['partner', 'treasury_account_id', 'gl_number', 'tr_count', 'tr_matched', 'br_count', 'br_matched'])
    #     for bank_acct in ares:
    #         partner, tr_acct_id, gl_number, tr_count, tr_matched, br_count, br_matched = process_acct(bank_acct, out_folder)
    #         csv_writer.writerow([partner, tr_acct_id, gl_number, tr_count, tr_matched, br_count, br_matched])


    in_folder = 'event'
    out_folder = 'recons_event'
    record_data_errors(in_folder, out_folder)
    ares = uf.get_lst_obj('data/active_bank_accounts_07', BankAcct)
    with open(f'data/{out_folder}/matching_summary.csv', "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['partner', 'treasury_account_id', 'gl_number', 'tr_count', 'tr_matched', 'br_count', 'br_matched'])
        for bank_acct in ares:
            partner, tr_acct_id, gl_number, tr_count, tr_matched, br_count, br_matched = process_acct(bank_acct, in_folder,out_folder)
            csv_writer.writerow([partner, tr_acct_id, gl_number, tr_count, tr_matched, br_count, br_matched])

if __name__ == '__main__':
    main()
