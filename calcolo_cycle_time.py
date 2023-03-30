import pandas as pd
import re
import numpy as np
from crea_df import *

#N.B. per calcolare il cycle time creare prima il df con crea_dataframe_cycle_time_tagliato(filename), poi richiama calcolo_cycle_time

# conversione da rpm a secondi per giro considerando l'accelerazione (velocità media tra punto iniziale e finale)
def tempo_giro_acc(v_i, v_f):
    secondi_giro = 1 / (((v_i + v_f) / 2) / 60)
    return secondi_giro


# conversione da rpm a secondi per giro senza considerare l'accelerazione
def tempo_giro_no_acc(rpm):
    secondi_giro = 1 / (rpm / 60)
    return secondi_giro


# funzione che dato in ingresso la velocità iniziale, quella finale, la rampa e il numero di aghi ritorna
# il numero di giri (espresso sotto forma di poslin) con i quali si raggiunge la velocità finale
def calcola_poslin(v_i, v_f, acc, aghi):
    t = (int(v_f) - int(v_i)) / int(acc)
    v_m = (int(v_f) + int(v_i)) / 2
    rps = v_m / 60
    n_giri = rps * t
    poslin_raggiunta = n_giri * aghi
    return poslin_raggiunta


# funzione che calcola incrementalmente il cycle time in secondi.
def calcolo_cycle_time(df, filename):
    n_aghi = leggi_n_aghi(filename)
    cycle_time = float(0)  # variabile che conterrà il tempo complessivo
    for i, row in df.iterrows():
        # per la prima riga del df pongo cycle_time=0
        if i == 0:
            cycle_time += 0.0
        else:
            if i == 1:  # dato che la macchina parte da ferma considero la vecolità media tra 0 e la velcoità letta nella prima riga del df
                previous_poslin = float(df.loc[i - 1, "posLin"])
                previous_rpm = float(df.loc[i - 1, "rpm_azionam"])
                actual_poslin = float(row["posLin"])
                cycle_time += tempo_giro_acc(0, previous_rpm) * ((actual_poslin - previous_poslin) / n_aghi)
            else:  # incremento il cycle time senza considerare l'accelerazione
                previous_poslin = float(df.loc[i - 1, "posLin"])
                previous_rpm = float(df.loc[i - 1, "rpm_azionam"])
                actual_poslin = float(row["posLin"])
                cycle_time += tempo_giro_no_acc(previous_rpm) * ((actual_poslin - previous_poslin) / n_aghi)
    return cycle_time


# cerca nell'artcode l'informazione relativa al numero di aghi. contenuta dopo la stringa "n_aghi_pr_c". (ci sono più stringhe uguali)
def leggi_n_aghi(filename):
    pattern_aghi = re.compile(r"^n_aghi_pr_c")
    trovato = False
    with open(filename, 'r') as f:
        for line in f:
            if trovato:#uso per bloccare la lettura al primo "n_aghi_pr_c" individuato
                return aghi
            if re.search(pattern_aghi, line.strip()):
                aghi = int(line.split("=")[1])
                trovato = True


# crea un df contenente poslin e rpm_azionam (sono contenute le informazioni lette dagli azionamenti)
def crea_df_vel(filename):
    pattern_vel = re.compile(r"^AZIONAM_VEL_POSLIN /")
    pattern_rampa = re.compile(r"^AZIONAM_VEL_POSLIN_RAMPA")
    pattern_start_rampa = re.compile(r"^AZIONAM_START_EXEC_RAMPA")
    df_rows = []
    aghi = leggi_n_aghi(filename)
    vel_riga_prec = 0
    with open(filename, 'r') as f:
        for line in f:
            # se la riga è del tipo "^AZIONAM_VEL_POSLIN /", allora contiene le informazioni sulle velcotà giuste
            if re.search(pattern_vel, line.strip()):
                poslin = int(line.split("/")[1].split("=")[1])
                vel = int(line.split("/")[2].split("=")[1])
                vel_riga_prec = vel
                df_rows.append([poslin, vel])
            # se la riga è del tipo "^AZIONAM_VEL_POSLIN_RAMPA" allora contiene le info sulla rampa
            #perciò deve calcoalre quando verrà raggiunta a velocità specificata
            if re.search(pattern_rampa, line.strip()):
                start_rampa_trovato = False
                while start_rampa_trovato == False:
                    next_line = next(f, None)
                    # nella riga successiva (alcune volte non è quella sucessiva ma la seconda successiva)
                    # è riportata la poslin di inizio rampa.
                    # mentre nella riga corrente trovo le info di vel_iniziale, vel_finale e rampa
                    if re.search(pattern_start_rampa, next_line.strip()):
                        start_rampa_trovato = True
                        poslin = int(next_line.split("/")[1].split("=")[1])
                        vel = int(vel_riga_prec)
                        df_rows.append([poslin, vel])
                        rampa = int(line.split("/")[2].split("rampa=")[1].split(" ")[0])
                        vel_finale = int(line.split("/")[2].split("vel=")[1].split(" ")[0])
                        # calcolo a quale posizione lineare raggiungo la velcoità finale
                        poslin_calc = abs(calcola_poslin(vel, vel_finale, rampa, aghi))
                        df_rows.append([poslin + poslin_calc, vel_finale])
                        vel_riga_prec = vel_finale
    df_cycle = pd.DataFrame(df_rows, columns=["posLin", "rpm_azionam"])
    df_cycle = df_cycle.astype({"posLin": int, "rpm_azionam": int})
    return df_cycle


# Funzione che ritorna un df NON a rango unico ottentuo sfruttando art_forstep e azionamenti.
# rpm_azionam è ottimizzata per calcolare il cycle time
def crea_dataframe_cycle_time_tagliato(filename):
    df_step = crea_sub_df("step_actual", "step", filename)
    df_rango = crea_sub_df("rango_actual", "rango", filename)
    df_rpm = crea_sub_df("rpm_program", "rpm", filename)
    df_art_forstep = crea_sub_df("ART_FORSTEP_ECONOM_ACTUAL", "forstep_econ",filename)
    df = pd.DataFrame()
    df = df_step.merge(df_rango, how='outer', on="posLin", sort=True)
    # modifica df_rpm in modo da essere compatibile con step e rango e merge
    df_rpm_modificato = make_df_compatible_with_step(df_rpm, df)
    df = df.merge(df_rpm_modificato, how='outer', on="posLin", sort=True)
    # merge con df_forstep
    df = df.merge(df_art_forstep, how='outer', on="posLin", sort=True)
    #creo df_vel leggendo la parte degli azionamenti
    df_vel = crea_df_vel(filename)
    df = df.merge(df_vel, how='outer', on="posLin", sort=True)
    # gestion NaN
    df = df.fillna(method='ffill')  # ForwardFILL
    df = df.fillna(method='bfill')  # BackwardFILL
    # aggiungo al df la colonna codice_zona che è rappresentata dalla lista_codici appena creata
    # gestione delle righe con posLin duplicata: mantengo il primo elemento
    for valore_posLin in df['posLin'].unique():
        righe_duplicate = df[df['posLin'] == valore_posLin].duplicated(subset='posLin')
        if any(righe_duplicate):
            df = df.drop(df[df['posLin'] == valore_posLin][righe_duplicate].index[0])
    # assegno ad ogni colonna il tipo desiderato
    df = df.astype({"posLin": int,  "step": int, "rango": int, "rpm": int, "rpm_azionam": int})
    df = df.reset_index(drop=True)
    #rimuove le zone di gestione doppia e compatta la poslin (funzione definita in crea_df.py)
    df = rimuovi_zone_gestione_doppia(df)
    return df
