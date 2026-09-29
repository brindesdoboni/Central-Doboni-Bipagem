# Central Boni – Bipagem da produção

Registra **quem, onde e quando** cada pedido passou por: **Separação → Gravação (início/fim) → Expedição**, com alerta de **falta de material**. Painel só para você.

## Publicar no Railway (uma vez, ~10 min)
1. **GitHub**: crie um repositório privado `central-boni-bipagem` → *Add file → Upload files* → arraste TODOS os arquivos desta pasta (inclusive a pasta `web`) → *Commit*.
2. **Railway** (seu projeto): *New → GitHub Repo* → escolha o repositório.
3. No serviço criado: *Settings → Volumes → Add Volume* com **Mount path `/data`** (é onde fica o banco; sem isso os dados somem a cada atualização).
4. *Variables* → adicione:
   - `ADMIN_PASSWORD` = senha do seu painel
   - `STATION_KEY` = chave dos postos (digitada uma vez em cada PC de bipagem)
   - `API_TOKEN` = token que a automação usa para enviar os lotes
   - `SECRET_KEY` = qualquer texto longo aleatório
5. *Settings → Networking → Generate Domain*. Seu endereço fica tipo `https://central-boni.up.railway.app`.

## Primeiro uso
- `https://SEU-ENDERECO/painel` → senha → **Equipe** → cadastre cada colaborador → **Crachás** → imprima e plastifique (tem também os códigos **FALTA DE MATERIAL** e **DESFAZER** para colar em cada posto).
- Em cada PC com leitor USB abra `https://SEU-ENDERECO/bipar`, digite a chave do posto e escolha **Separação**, **Gravação** ou **Expedição** (fica salvo).

## Como a equipe bipa
1. Bipa o **crachá** (fica logado 20 min sem uso ou até outro crachá).
2. Bipa a **etiqueta** (o código de barras do rastreio já serve).
   - **Gravação**: 1º bipe = início, 2º bipe = fim (mede o tempo).
   - **Falta de material**: bipa `FALTA DE MATERIAL` e depois a etiqueta.
   - **Errou?** bipa `DESFAZER`.
3. Tela **verde** = ok · **amarela** = atenção · **vermelha** = erro (ex.: tentar expedir pedido não gravado é **bloqueado**).

## Enviar o lote do dia
Depois de gerar `spec.py` e o PDF das etiquetas:
```
python3 enviar_lote.py spec.py saida.pdf --url https://SEU-ENDERECO --token SEU_API_TOKEN
```
Reenviar o mesmo lote não duplica nada.

## Painel
Contagem por etapa e canal, alertas (falta de material, gravação parada há +30 min, pulou etapa), produtividade por colaborador (quantidade, tempo médio de gravação, 1º e último bipe), histórico de cada pedido, **Exportar CSV** (Excel) e **Backup** do banco.
