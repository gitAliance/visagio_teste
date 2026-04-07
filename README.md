# Dashboard V-Educa (Streamlit)

Aplicacao web para compartilhar o dashboard com outras pessoas via GitHub + Streamlit Cloud.

## Requisitos

- Python 3.10+
- Arquivo `base.xlsx` na raiz do projeto

## Executar localmente

1. Instale dependencias:

   ```bash
   pip install -r requirements.txt
   ```

2. Rode o app:

   ```bash
   streamlit run app.py
   ```

3. Abra o link local mostrado no terminal.

## Publicar no GitHub

1. Crie um repositorio no GitHub.
2. Envie estes arquivos:
   - `app.py`
   - `requirements.txt`
   - `base.xlsx`
   - `README.md`
3. Faça push da branch principal.

## Publicar no Streamlit Cloud

1. Acesse https://share.streamlit.io
2. Conecte sua conta GitHub.
3. Selecione o repositorio e branch.
4. Defina `app.py` como Main file path.
5. Clique em Deploy.

A cada novo push no GitHub, o deploy sera atualizado automaticamente.
