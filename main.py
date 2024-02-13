from sqlalchemy import create_engine
import json
import requests
import pandas as pd
from azure.identity import AzureCliCredential


def obter_token():
    credencial = AzureCliCredential()
    escopo = "https://forms.office.com/.default"
    token = credencial.get_token(escopo)
    return token.token


def obter_dados_formulario(token):
    headers = {"Authorization": f"Bearer {token}"}
    link_formularios = "https://forms.office.com/formapi/api/forms"
    r = requests.get(link_formularios, headers=headers)
    return r.json()


def obter_respostas(token, owner_tenant_id, owner_id, form_id):
    headers = {"Authorization": f"Bearer {token}"}
    link_respostas = f"https://forms.office.com/formapi/api/{owner_tenant_id}/users/{owner_id}/forms('{form_id}')/responses"
    r = requests.get(link_respostas, headers=headers)
    return r.json()["value"]


def criar_dataframe_respostas(respostas):
    df_respostas = pd.DataFrame({
        'submitDate': [resposta['submitDate'] for resposta in respostas],
        'responder': [resposta['responder'] for resposta in respostas],
        'responderName': [resposta['responderName'] for resposta in respostas],
        'answers': [json.loads(resposta['answers'])[0] for resposta in respostas],
    })

    df_respostas[['answer1', 'questionId']] = pd.json_normalize(df_respostas['answers'])
    df_respostas.drop(columns=['answers'], inplace=True)

    return df_respostas


def salvar_no_postgres(dataframe, engine):
    dataframe.to_sql('respostas', con=engine, index=False, if_exists="replace")


def salvar_em_csv(dataframe, caminho, sep="|"):
    dataframe.to_csv(caminho, index=False, sep=sep)


def consultar_sql(engine, consulta):
    with engine.connect() as connection:
        resultado = connection.execute(consulta)
        df_resultado = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return df_resultado


def main():
    token_acesso = obter_token()
    dados_formulario = obter_dados_formulario(token_acesso)

    formulario = dados_formulario["value"][0]
    form_id = formulario["id"]
    owner_id = formulario["ownerId"]
    owner_tenant_id = formulario["ownerTenantId"]

    respostas = obter_respostas(token_acesso, owner_tenant_id, owner_id, form_id)

    df_respostas = criar_dataframe_respostas(respostas)

    engine = create_engine("postgresql://postgres:postgres@127.0.0.1:5432/db_nps", echo=True)

    salvar_no_postgres(df_respostas, engine)
    salvar_em_csv(df_respostas, r'C:\Users\carlos.aguirre\Desktop\e-NPS\respostas.csv', sep="|")

    # Exemplo de consulta SQL
    consulta_sql = "SELECT responder FROM respostas"
    resultado_sql = consultar_sql(engine, consulta_sql)
    print(resultado_sql)
    print("Tal alteração")


if __name__ == "__main__":
    main()
