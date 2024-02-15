let
    Fonte = Csv.Document(File.Contents("C:\Users\carlos.aguirre\Desktop\e-NPS\respostas.csv"), [Delimiter="|", Columns=5, QuoteStyle=QuoteStyle.None]),
    #"Cabeçalhos Promovidos" = Table.PromoteHeaders(Fonte, [PromoteAllScalars=true]),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Cabeçalhos Promovidos", {{"submitDate", type datetime}, {"responder", type text}, {"responderName", type text}, {"answer1", Int64.Type}, {"questionId", type text}}),
    #"Tipo Alterado1" = Table.TransformColumnTypes(#"Tipo Alterado", {{"submitDate", type date}}),
    #"Valor Substituído" = Table.ReplaceValue(#"Tipo Alterado1", "", "anonymous", Replacer.ReplaceValue, {"responderName"}),
    Categoria = Table.AddColumn(#"Valor Substituído", "Categoria", each 
        if [answer1] >= 0 and [answer1] <= 6 then "Detratores" 
        else if [answer1] >= 7 and [answer1] <= 8 then "Passivos" 
        else if [answer1] >= 9 and [answer1] <= 10 then "Promotores" 
        else null
    ),
    #"Colunas Renomeadas" = Table.RenameColumns(Categoria, {
        {"submitDate", "Data"},
        {"responder", "E-mail Responder"},
        {"responderName", "Responder Name"},
        {"answer1", "Nota"},
        {"questionId", "Id Pergunta"}
    }),
  
    #"Dia Inserido" = Table.AddColumn(#"Colunas Renomeadas", "Dia", each Date.Day([Data]), Int64.Type),
    #"Mês Inserido" = Table.AddColumn(#"Dia Inserido", "Mês", each Date.Month([Data]), Int64.Type),
    #"Ano Inserido" = Table.AddColumn(#"Mês Inserido", "Ano", each Date.Year([Data]), Int64.Type),
    #"Nome do Mês Inserido" = Table.AddColumn(#"Ano Inserido", "Nome do Mês", each Date.MonthName([Data]), type text),
    #"Primeiros caracteres extraídos" = Table.TransformColumns(#"Nome do Mês Inserido", {{"Nome do Mês", each Text.Start(_, 3), type text}}),
    #"Colocar Cada Palavra Em Maiúscula" = Table.TransformColumns(#"Primeiros caracteres extraídos", {{"Nome do Mês", Text.Proper, type text}}),
    #"Coluna Condicional Adicionada" = Table.AddColumn(#"Colocar Cada Palavra Em Maiúscula", "Bimestre Ano", each 
        if [Mês] <= 6 then "01B" else "02B"
    ),
    #"Últimos caracteres inseridos" = Table.AddColumn(#"Coluna Condicional Adicionada", "Últimos caracteres", each Text.End(Text.From([Ano], "pt-BR"), 2), type text),
    #"Coluna Mesclada Inserida" = Table.AddColumn(#"Últimos caracteres inseridos", "Bimestre Ano.1", each Text.Combine({[Bimestre Ano], [Últimos caracteres]}, ""), type text),
    #"Tipo Alterado3" = Table.TransformColumnTypes(#"Coluna Mesclada Inserida", {{"Bimestre Ano", type text}}),
    #"Valor Substituído1" = Table.ReplaceValue(#"Tipo Alterado3", "B", "", Replacer.ReplaceText, {"Bimestre Ano"}),
    #"Tipo Alterado4" = Table.TransformColumnTypes(#"Valor Substituído1", {{"Bimestre Ano", Int64.Type}, {"Últimos caracteres", Int64.Type}}),
    #"Colunas Mescladas" = Table.CombineColumns(Table.TransformColumnTypes(#"Tipo Alterado4", {{"Bimestre Ano", type text}, {"Últimos caracteres", type text}}, "pt-BR"), {"Bimestre Ano", "Últimos caracteres"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "Order Bimestre"),
    #"Colunas Renomeadas2" = Table.RenameColumns(#"Colunas Mescladas", {{"Bimestre Ano.1", "Bimestre Ano"}}),
    #"Dia da Semana Inserido" = Table.AddColumn(#"Colunas Renomeadas2", "Dia da Semana", each Date.DayOfWeek([Data]), Int64.Type),
    #"Coluna Duplicada" = Table.DuplicateColumn(#"Dia da Semana Inserido", "Ano", "Ano - Copiar"),
    #"Últimos caracteres extraídos1" = Table.TransformColumns(#"Coluna Duplicada", {{"Ano - Copiar", each Text.End(Text.From(_, "pt-BR"), 2), type text}}),
    #"Coluna Mesclada Inserida2" = Table.AddColumn(#"Últimos caracteres extraídos1", "MêsAno", each Text.Combine({[Nome do Mês], [#"Ano - Copiar"]}, "-"), type text),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Coluna Mesclada Inserida2", {"Ano - Copiar"}),
    #"Semana do Ano Inserida" = Table.AddColumn(#"Colunas Removidas1", "Semana do Ano", each Date.WeekOfYear([Data]), Int64.Type),
    #"Personalização Adicionada" = Table.AddColumn(#"Semana do Ano Inserida", "Bimestre", each "0" & Text.Start([Bimestre Ano], 2), type text),
    MesAtual = Table.AddColumn(#"Personalização Adicionada", "Mês Atual", each 
        if [Mês] = Date.Month(DateTime.LocalNow()) and [Ano] = Date.Year(DateTime.LocalNow()) then "MêsAtual" else [MêsAno], type text),
    OrderMesAtual = Table.AddColumn(
        MesAtual, "OrderMesAtual", each 
        if [Mês Atual] = "MêsAtual" then 20230.5 else Number.From( Text.From(Date.Year([Data])) & Text.From(Date.Month([Data])) ), type number
    ),
    OrderMesAno = Table.AddColumn(
        OrderMesAtual, "OrderMesAno", each 
        Number.From( Text.From(Date.Year([Data])) & Text.From(Date.Month([Data])) ), type number
    )
in
    OrderMesAno