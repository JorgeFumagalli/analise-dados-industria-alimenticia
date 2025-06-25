# -- coding: utf-8 --
"""
Script profissional para tratamento de dados de carteira de vendas aberta,
com enriquecimento via tabelas auxiliares e preparação para análise e produção.

Autor: Jorge Luiz Fumagalli
"""

import pandas as pd


def carregar_planilhas(principal_path, de_para_path):
    df_principal = pd.read_excel(principal_path)
    df_de_para = pd.read_excel(de_para_path)
    df_principal['CodItem'] = df_principal['CodItem'].astype(str)
    df_de_para['CodItem'] = df_de_para['CodItem'].astype(str)
    return df_principal, df_de_para


def mesclar_com_setor(df_principal, df_de_para):
    return pd.merge(df_principal, df_de_para[['CodItem', 'Setor']], on='CodItem', how='left')


def carregar_abertos(path):
    df = pd.read_excel(path)
    df['Pedido'] = df['Pedido'].apply(lambda x: str(int(float(x))) if '.' in str(x) else str(x)).str.strip()
    return df


def tratar_e_enriquecer(df_base, df_abertos):
    df_base['NrPedvenda'] = df_base['NrPedvenda'].astype(str).str.strip()
    df_abertos['Pedido'] = df_abertos['Pedido'].astype(str).str.strip()

    df = pd.merge(df_base, df_abertos[['Pedido', 'Previsão Faturamento/Embarque', 'Valor']],
                  left_on='NrPedvenda', right_on='Pedido', how='left')

    df['Entrega'] = df['Previsão Faturamento/Embarque']
    df = df[df['CodPessoa'] != 7238]
    df = df[df['NrPedvenda'] != '251464']

    df['Valor do Item'] = df['QtdePedida'] * df['ValLiquido']
    df['Descricao'] = (df['ItDescricao'] + df['RfDescricao']).str.strip().str.replace(r'\s+', ' ', regex=True)
    df['NrPedvenda'] = df['NrPedvenda'].astype(int)

    df = df.drop_duplicates(subset=['NrPedvenda', 'CodItem', 'RefSeq', 'EmbSequencia'], keep='first')

    colunas_selecionadas = [1, 27, 23, 3, 6, 7, 8, 29, 11, 15, 18, 28, 26, 21]
    df = df.iloc[:, colunas_selecionadas]
    return df


def main():
    # Caminhos dos arquivos
    planilha_principal_path = "./planilha_principal.xlsx"
    planilha_de_para_path = "./de_para.xlsx"
    relatorio_aberto_path = "./pedidos_em_aberto.xlsx"
    output_path_1 = "./completa_com_setor.xlsx"
    output_path_2 = "./completa_com_data.xlsx"

    # Etapa 1: Carregamento e merge com setor
    df_principal, df_de_para = carregar_planilhas(planilha_principal_path, planilha_de_para_path)
    df_com_setor = mesclar_com_setor(df_principal, df_de_para)
    df_com_setor.to_excel(output_path_1, index=False)
    print(f"✓ Planilha com setor salva: {output_path_1}")

    # Etapa 2: Enriquecimento com previsão de entrega
    df_abertos = carregar_abertos(relatorio_aberto_path)
    df_final = tratar_e_enriquecer(df_com_setor, df_abertos)
    df_final.to_excel(output_path_2, index=False)
    print(f"✓ Planilha final salva com sucesso: {output_path_2}")


if __name__ == "__main__":
    main()
