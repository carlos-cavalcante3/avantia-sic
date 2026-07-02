# ADR 0001 — Transformação em DataFrame

## Status
Aceito

## Contexto
É preciso descobrir a melhor forma de extrair os dados da api sem perdas.

## Decisão
Transformar os dados extraídos pelo extract em um dataframe via *pandas* e carregando os dataframes como csv no banco. Os dados se adequam perfeitamente as colunas do formato csv.

## Alternativas consideradas
- Usar extração comum transformando apenas as necessidades e carregando normalmente -> Dados comumente sumiam ou não eram carregados corretamente 
- Baixar direto do site do RD Station os dados como csv -> Depender de extração via webscrapping mais complexa

## Consequências
- Os dados são todos registrados corretamente
- Não há perda durante o processo de extração inicial