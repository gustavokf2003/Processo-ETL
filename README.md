## Link do dashboard criado usando os dados transofrmados

https://lookerstudio.google.com/reporting/f6ad3135-4f05-476d-ab37-a32565ce0734/page/p_39ldqd44hd

## PROJETO DE ETL COM AIRFLOW

# Tutorial para rodar

Instale os requerimentos:

```bash
make install
```

Precisará ter o postgres instalado para rodar:

```bash
make create_db
```

Para rodar execute:

```bash
make run
```

Ao rodar abra o link do webserver que aparecer no terminal, filtre pela tag 'etl' e clique botao em baixo de actions. 

Se modificou o arquivo dag é necessário matar o terminal que estava rodando o airflow (Crtl + c) e rodar novamente.
