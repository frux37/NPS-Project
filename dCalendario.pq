let
    Qtde = Duration.Days(Max-Min)+1,
    Data = List.Dates(Min,Qtde,#duration(1,0,0,0)),
    #"Convertido para Tabela" = Table.FromList(Data, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Colunas Renomeadas" = Table.RenameColumns(#"Convertido para Tabela",{{"Column1", "Data"}}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Colunas Renomeadas",{{"Data", type date}}),
    #"Dia Inserido" = Table.AddColumn(#"Tipo Alterado", "Dia", each Date.Day([Data]), Int64.Type),
    #"Mês Inserido" = Table.AddColumn(#"Dia Inserido", "Mês", each Date.Month([Data]), Int64.Type),
    #"Ano Inserido" = Table.AddColumn(#"Mês Inserido", "Ano", each Date.Year([Data]), Int64.Type),
    #"Nome do Mês Inserido" = Table.AddColumn(#"Ano Inserido", "Nome do Mês", each Date.MonthName([Data]), type text),
    #"Primeiros caracteres extraídos" = Table.TransformColumns(#"Nome do Mês Inserido", {{"Nome do Mês", each Text.Start(_, 3), type text}}),
    #"Colocar Cada Palavra Em Maiúscula" = Table.TransformColumns(#"Primeiros caracteres extraídos",{{"Nome do Mês", Text.Proper, type text}}),
    #"Coluna Condicional Adicionada" = Table.AddColumn(#"Colocar Cada Palavra Em Maiúscula", "Bimestre Ano", each if [Mês] = 1 then "01B" else if [Mês] = 2 then "01B" else if [Mês] = 3 then "02B" else if [Mês] = 4 then "02B" else if [Mês] = 5 then "03B" else if [Mês] = 6 then "03B" else if [Mês] = 7 then "04B" else if [Mês] = 8 then "04B" else if [Mês] = 9 then "05B" else if [Mês] = 10 then "05B" else if [Mês] = 11 then "06B" else if [Mês] = 12 then "06B" else null),
    #"Últimos caracteres inseridos" = Table.AddColumn(#"Coluna Condicional Adicionada", "Últimos caracteres", each Text.End(Text.From([Ano], "pt-BR"), 2), type text),
    #"Coluna Mesclada Inserida" = Table.AddColumn(#"Últimos caracteres inseridos", "Bimestre Ano.1", each Text.Combine({[Bimestre Ano], [Últimos caracteres]}, ""), type text),
    #"Tipo Alterado1" = Table.TransformColumnTypes(#"Coluna Mesclada Inserida",{{"Bimestre Ano", type text}}),
    #"Valor Substituído" = Table.ReplaceValue(#"Tipo Alterado1","B","",Replacer.ReplaceText,{"Bimestre Ano"}),
    #"Tipo Alterado2" = Table.TransformColumnTypes(#"Valor Substituído",{{"Bimestre Ano", Int64.Type}, {"Últimos caracteres", Int64.Type}}),
    #"Colunas Mescladas" = Table.CombineColumns(Table.TransformColumnTypes(#"Tipo Alterado2", {{"Bimestre Ano", type text}, {"Últimos caracteres", type text}}, "pt-BR"),{"Bimestre Ano", "Últimos caracteres"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"Order Bimestre"),
    #"Colunas Renomeadas1" = Table.RenameColumns(#"Colunas Mescladas",{{"Bimestre Ano.1", "Bimestre Ano"}}),
    #"Dia da Semana Inserido" = Table.AddColumn(#"Colunas Renomeadas1", "Dia da Semana", each Date.DayOfWeek([Data]), Int64.Type),
    #"Coluna Condicional Adicionada1" = Table.AddColumn(#"Dia da Semana Inserido", "Quantidade", each if [Dia da Semana] = 0 then 0 else if [Dia da Semana] = 6 then 0 else 1),
    #"Tipo Alterado3" = Table.TransformColumnTypes(#"Coluna Condicional Adicionada1",{{"Quantidade", Int64.Type}}),
    #"Coluna Duplicada" = Table.DuplicateColumn(#"Tipo Alterado3", "Ano", "Ano - Copiar"),
    #"Últimos caracteres extraídos" = Table.TransformColumns(#"Coluna Duplicada", {{"Ano - Copiar", each Text.End(Text.From(_, "pt-BR"), 2), type text}}),
    #"Coluna Mesclada Inserida1" = Table.AddColumn(#"Últimos caracteres extraídos", "MêsAno", each Text.Combine({[Nome do Mês], [#"Ano - Copiar"]}, "-"), type text),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Coluna Mesclada Inserida1",{"Ano - Copiar"}),
    #"Semana do Ano Inserida" = Table.AddColumn(#"Colunas Removidas1", "Semana do Ano", each Date.WeekOfYear([Data]), Int64.Type),
    #"Personalização Adicionada" = Table.AddColumn(#"Semana do Ano Inserida", "Bimestre", each "0"&Text.Start([Bimestre Ano], 2), type text),
    MesAtual = 
         Table.AddColumn(#"Personalização Adicionada", "Mês Atual", each 
         if ([Mês] = Date.Month(DateTime.LocalNow()) and [Ano] = Date.Year(DateTime.LocalNow()))
            then "MêsAtual"
            else [MêsAno], type text), 
     OrderMesAtual = 
          Table.AddColumn(
               MesAtual, "OrderMesAtual", 
               each 
               if [Mês Atual] = "MêsAtual" then 20230.5 else Number.From( Text.From(Date.Year([Data]))&Text.From(Date.Month([Data])) ), type number
          ), 
     OrderMesAno =
          Table.AddColumn(
               OrderMesAtual, "OrderMesAno", 
               each 
               Number.From( Text.From(Date.Year([Data]))&Text.From(Date.Month([Data])) ), type number
          )
in
    OrderMesAno