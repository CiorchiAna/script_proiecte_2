import pandas as pd
import time

# pandas config
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 100)

# configs
seed = 15032024

# metrics
start_time = time.time()

df = pd.read_csv('data/lp3_proiecte_optiuni.csv')

# new attributes
df['grupa'] = df['Echipa'].apply(lambda x: x.split('-')[0])
df['domenii'] = df['Optiuni'].apply(lambda x: x.split(',') if not pd.isna(x) else [])
df['d1'] = df['domenii'].apply(lambda x: x[0].split('-')[0] if len(x) > 0 else '')
df['d2'] = df['domenii'].apply(lambda x: x[1].split('-')[0] if len(x) > 1 else '')
df['d3'] = df['domenii'].apply(lambda x: x[2].split('-')[0] if len(x) > 2 else '')

# correct duplicate domains
df['d3'] = df.apply(lambda x: x['d3'] if x['d3'] not in (x['d1'], x['d2']) else '', axis=1)
df['d2'], df['d3'] = zip(*df.apply(lambda x: (x['d3'], '') if x['d1'] == x['d2'] else (x['d2'], x['d3']), axis=1))

#inițializare coloane de alocare
df['alocare'] = ''
df['runda'] = ''
allocations = {'1': {}, '2': {}, '3': {}}

#amestecare dataframe pentru a asigura randomizarea grupurilor
df = df.sample(frac=1, random_state=seed).reset_index(drop=True)


#funcție creată pentru verificarea unui domeniu
def can_allocate(group, domain, round_num):
    return domain and domain not in allocations[round_num].get(group, [])


#funcție definită pentru alocare
def allocate(row, round_num):
    group = row['grupa']
    d1, d2, d3 = row['d1'], row['d2'], row['d3']

    if round_num == 1 and can_allocate(group, d1, '1'):
        allocations['1'].setdefault(group, []).append(d1)
        return d1, '1'
    elif round_num == 2 and can_allocate(group, d2, '2'):
        allocations['2'].setdefault(group, []).append(d2)
        return d2, '2'
    elif round_num == 3 and can_allocate(group, d3, '3'):
        allocations['3'].setdefault(group, []).append(d3)
        return d3, '3'
    return '', ''


#alocări printr-o singură trecere
for index, row in df.iterrows():
    alocare, runda = allocate(row, 1)
    if not alocare:
        alocare, runda = allocate(row, 2)
    if not alocare:
        alocare, runda = allocate(row, 3)

    df.at[index, 'alocare'] = alocare
    df.at[index, 'runda'] = runda

# tema alocata
df['tema_proiect'] = df.apply(
    lambda x: next((t for t in x['domenii'] if x['alocare'] and t.startswith(x['alocare'])),
                   'De alocat manual'), axis=1)

# save results
print(df)
df.to_csv('data/alocari_teme.csv', index=False)

# metrics
execution_time = time.time() - start_time
print("Total execution time %s" % execution_time)
