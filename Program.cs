using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using System.Data.SqlClient;
using System.Text;
using System.Globalization;
using System.Text.RegularExpressions;

namespace SimpleTimeService
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            IHost host = Host.CreateDefaultBuilder(args)
                .UseWindowsService()
                .ConfigureServices(services =>
                {
                    services.AddHostedService<TimeService>();
                })
                .Build();

            await host.RunAsync();
        }

        public class Config
        {
            public string RunAt { get; set; }
            public string ApiUrl { get; set; }
            public string ConnectionString { get; set; }
            public string Mode { get; set; }
            public string StartDate { get; set; }
            public string EndDate { get; set; }
            public string ResendDocs { get; set; }
            public string ResendFrom { get; set; }
            public string ResendTo { get; set; }
            public AuthorizationConfig Authorization { get; set; }
        }

        public class AuthorizationConfig
        {
            public string username { get; set; }
            public string password { get; set; }
            public string company { get; set; }
            public string instance { get; set; }
            public string grant_type { get; set; }
            public string line { get; set; }
            public string secondToken { get; set; }

            // Adicione isto:
            public string firstToken { get; set; }
        }

        public class TimeService : BackgroundService
        {
            private Timer _timer;
            private Config _config;
            private bool _hasExecuted = false;

            public override async Task StartAsync(CancellationToken cancellationToken)
            {
                Notas.Log("Serviço iniciando...");
                await LoadConfigAsync();
                OnStart();
                await base.StartAsync(cancellationToken);
            }

            protected override Task ExecuteAsync(CancellationToken stoppingToken)
            {
                _timer = new Timer(DoWork, null, TimeSpan.Zero, TimeSpan.FromMinutes(1));
                return Task.CompletedTask;
            }

            public override Task StopAsync(CancellationToken cancellationToken)
            {
                OnStop();
                _timer?.Change(Timeout.Infinite, 0);
                return base.StopAsync(cancellationToken);
            }

            public override void Dispose()
            {
                _timer?.Dispose();
                base.Dispose();
            }

            private void OnStart()
            {
                Console.WriteLine("Service started.");
                Notas.Log("Serviço iniciado.");
            }

            private void OnStop()
            {
                Console.WriteLine("Service stopped.");
                Notas.Log("Serviço parado.");
            }

            private async Task LoadConfigAsync()
            {
                try
                {
                    string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.json");
                    string json = await File.ReadAllTextAsync(path);

                    _config = JsonSerializer.Deserialize<Config>(json, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                    if (_config == null || string.IsNullOrWhiteSpace(_config.RunAt) || string.IsNullOrWhiteSpace(_config.ApiUrl))
                        throw new Exception("Valores inválidos ou ausentes no config.");

                    Notas.Log("Configuração carregada com sucesso.");
                }
                catch (Exception ex)
                {
                    string msg = $"Erro ao carregar configuração: {ex.Message}";
                    Console.WriteLine(msg);
                    Notas.Log(msg);
                    Environment.Exit(1);
                }
            }

            private async void DoWork(object state)
            {
                string currentTime = DateTime.Now.ToString("HH:mm");

                if (currentTime == _config.RunAt)
                {
                    if (!_hasExecuted)
                    {
                        Notas.Log($"Executando tarefa agendada para {currentTime}.");
                        await CallApiAsync();
                        _hasExecuted = true;
                    }
                }
                else
                {
                    _hasExecuted = false;
                }
            }

            private async Task CallApiAsync(List<(string Nome, string Ref, string Documento, string SAFT, string SAFTPMS, string SerieFiscal, string SerieFiscalPMS, string Entidade, DateTime DataVenc, string CondPag, string DadosAssinatura, string Hash, string ModPag, double TotalMerc, double TotalIva, string NumContribuinte, string Morada, string CodigoPostal, string Localidade, string Distrito, string Pais, int Numero)> documentosPendentes = null)
            {
                try
                {
                    using var client = new HttpClient();
                    var url = $"{_config.ApiUrl}/token";

                    var content = new FormUrlEncodedContent(new[]
                    {
            new KeyValuePair<string, string>("username", _config.Authorization.username),
            new KeyValuePair<string, string>("password", _config.Authorization.password),
            new KeyValuePair<string, string>("company", _config.Authorization.company),
            new KeyValuePair<string, string>("instance", _config.Authorization.instance),
            new KeyValuePair<string, string>("grant_type", _config.Authorization.grant_type),
            new KeyValuePair<string, string>("line", _config.Authorization.line)
        });

                    Notas.Log("Enviando requisição para API...");

                    var response = await client.PostAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        string responseBody = await response.Content.ReadAsStringAsync();
                        using var doc = JsonDocument.Parse(responseBody);
                        string accessToken = doc.RootElement.GetProperty("access_token").GetString();

                        Notas.Log($"API respondeu com sucesso. Novo token: {accessToken}");

                        _config.Authorization.secondToken = accessToken;

                        string updatedJson = JsonSerializer.Serialize(_config, new JsonSerializerOptions { WriteIndented = true });
                        await File.WriteAllTextAsync("config.json", updatedJson);

                        Notas.Log("secondToken salvo em config.json com sucesso.");

                        // Retentar envio após renovar token
                        await CreateDocumentAsync(documentosPendentes);
                    }
                    else
                    {
                        string failMsg = $"Erro na resposta da API: {response.StatusCode}";
                        Console.WriteLine(failMsg);
                        Notas.Log(failMsg);
                    }
                }
                catch (Exception ex)
                {
                    string errorMsg = $"Exceção durante chamada de API: {ex.Message}";
                    Console.WriteLine(errorMsg);
                    Notas.Log(errorMsg);
                }
            }


            private async Task CreateDocumentAsync(List<(string Nome, string Ref, string Documento, string SAFT, string SAFTPMS, string SerieFiscal, string SerieFiscalPMS, string Entidade, DateTime DataVenc, string CondPag, string DadosAssinatura, string Hash, string ModPag, double TotalMerc, double TotalIva, string NumContribuinte, string Morada, string CodigoPostal, string Localidade, string Distrito, string Pais, int Numero)> documentosPendentes = null)
            {
                try
                {
                    string connectionString = _config.ConnectionString;

                    using var connection = new SqlConnection(connectionString);
                    await connection.OpenAsync();

                    // 🔹 Primeiro, reenviar documentos que falharam anteriormente
                    var docsParaReenviar = new List<int>();
                    string selectFalhasQuery = @"
            SELECT ref 
            FROM dbo.CBLLogs 
            WHERE status != 200";

                    using (var cmdFalhas = new SqlCommand(selectFalhasQuery, connection))
                    using (var readerFalhas = await cmdFalhas.ExecuteReaderAsync())
                    {
                        while (await readerFalhas.ReadAsync())
                        {
                            docsParaReenviar.Add(readerFalhas.GetInt32(0));
                        }
                    }

                    if (docsParaReenviar.Any())
                    {
                        Notas.Log($"Encontrados {docsParaReenviar.Count} documentos para reenvio antes de criar novos.");
                        foreach (var refDoc in docsParaReenviar)
                        {
                            await ReenviarDocumentoAsync(refDoc);
                        }
                    }

                    List<(string Nome, string Ref, string Documento, string SAFT, string SAFTPMS, string SerieFiscal, string SerieFiscalPMS, string Entidade, DateTime DataVenc, string CondPag, string DadosAssinatura, string Hash, string ModPag, double TotalMerc, double TotalIva, string NumContribuinte, string Morada, string CodigoPostal, string Localidade, string Distrito, string Pais, int Numero)> documentos;

                    if (documentosPendentes != null)
                    {
                        documentos = documentosPendentes;
                    }
                    else
                    {
                        // Definir intervalo de datas com base no modo
                        DateTime startDate, endDate;

                        if (_config.Mode.Equals("Diario", StringComparison.OrdinalIgnoreCase))
                        {
                            var diaAnterior = DateTime.Today.AddDays(-1);
                            startDate = diaAnterior.Date;
                            endDate = diaAnterior.Date;
                        }
                        else if (_config.Mode.Equals("Personalizado", StringComparison.OrdinalIgnoreCase))
                        {
                            if (!DateTime.TryParse(_config.StartDate, out startDate) ||
                                !DateTime.TryParse(_config.EndDate, out endDate))
                            {
                                Notas.Log("Datas inválidas no modo Personalizado.");
                                return;
                            }

                            startDate = startDate.Date;
                            endDate = endDate.Date;
                        }
                        else
                        {
                            Notas.Log($"Modo '{_config.Mode}' não reconhecido. Cancelando operação.");
                            return;
                        }

                        var selectCabQuery = @"
                SELECT ref, documento, SAFT, SAFTPMS, SerieFiscal, SerieFiscalPMS, entidade, data, DadosAssinatura, Hash, CondPag, ModPag, TotalLiq, TotIVA, nif, nome,
       morada, codigopostal, localidade, distrito, pais, Numero
                FROM protelsevero.proteluser.V_CBLCabDocumentos
                WHERE CAST([data] AS DATE) BETWEEN @startDate AND @endDate
                ORDER BY data ASC";

                        var cabCommand = new SqlCommand(selectCabQuery, connection);
                        cabCommand.Parameters.AddWithValue("@startDate", startDate);
                        cabCommand.Parameters.AddWithValue("@endDate", endDate);

                        using var reader = await cabCommand.ExecuteReaderAsync();

                        documentos = new();

                        string SafeString(object value, string defaultValue) =>
                                                string.IsNullOrWhiteSpace(value as string) ? defaultValue : (value as string);

                        string NormalizePostalCode(string raw)
                        {
                            var digits = Regex.Replace(raw ?? "", @"[^\d]", ""); // remove tudo que não for número
                            if (digits.Length == 7)
                                return digits.Substring(0, 4) + "-" + digits.Substring(4, 3);
                            return "0000-000"; // fallback
                        }

                        while (await reader.ReadAsync())
                        {
                            string nifOriginal = reader["nif"].ToString();
                            string nifNumerico = Regex.Replace(nifOriginal, @"[^\d]", "");

                            string morada = string.IsNullOrWhiteSpace(reader["morada"]?.ToString()) ? "" : reader["morada"].ToString();
                            string rawCodPostal = reader["codigopostal"]?.ToString() ?? "";
                            string codigopostal = NormalizePostalCode(SafeString(rawCodPostal, ""));
                            string localidade = string.IsNullOrWhiteSpace(reader["localidade"]?.ToString()) ? "PRT" : reader["localidade"].ToString();
                            string distrito = reader["distrito"]?.ToString() ?? "";
                            string pais = string.IsNullOrWhiteSpace(reader["pais"]?.ToString()) ? "PT" : reader["pais"].ToString();

                            documentos.Add((
                                reader["nome"].ToString(),
                                reader["ref"].ToString(),
                                reader["documento"].ToString(),
                                reader["SAFT"].ToString(),
                                reader["SAFTPMS"].ToString(),
                                reader["SerieFiscal"].ToString(),
                                reader["SerieFiscalPMS"].ToString(),
                                reader["entidade"].ToString(),
                                ((DateTimeOffset)reader["data"]).DateTime,
                                reader["CondPag"].ToString(),
                                reader["DadosAssinatura"].ToString(),
                                reader["Hash"].ToString(),
                                reader["ModPag"].ToString(),
                                reader["TotalLiq"] != DBNull.Value ? Convert.ToDouble(reader["TotalLiq"]) : 0,
                                reader["TotIVA"] != DBNull.Value ? Convert.ToDouble(reader["TotIVA"]) : 0,
                                nifNumerico,
                                morada,
                                codigopostal,
                                localidade,
                                distrito,
                                pais,
                                reader["Numero"] != DBNull.Value ? Convert.ToInt32(reader["Numero"]) : 0
                            ));
                        }

                        reader.Close();

                        if (!documentos.Any())
                        {
                            Notas.Log($"Nenhum documento encontrado entre {startDate:yyyy-MM-dd} e {endDate:yyyy-MM-dd}.");
                            return;
                        }
                    }

                    using var client = new HttpClient();
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _config.Authorization.secondToken);
                    var url = $"{_config.ApiUrl}/Palacete/Vendas/";

                    var documentosAFalhar = new List<(string Nome, string Ref, string Documento, string SAFT, string SAFTPMS, string SerieFiscal, string SerieFiscalPMS, string Entidade, DateTime DataVenc, string CondPag, string DadosAssinatura, string Hash, string ModPag, double TotalMerc, double TotalIva, string NumContribuinte, string Morada, string CodigoPostal, string Localidade, string Distrito, string Pais, int Numero)>();

                    foreach (var doc in documentos)
                    {
                        // ✅ ADICIONAR AQUI A CHECAGEM
                        string checkQuery = "SELECT COUNT(1) FROM dbo.CBLLogs WHERE ref = @ref";
                        using (var checkCmd = new SqlCommand(checkQuery, connection))
                        {
                            checkCmd.Parameters.AddWithValue("@ref", doc.Ref);
                            int exists = (int)await checkCmd.ExecuteScalarAsync();
                            if (exists > 0)
                            {
                                Notas.Log($"Documento {doc.Ref} já enviado anteriormente, ignorado.");
                                continue;
                            }
                        }

                        Notas.Log($"Documento: {doc.Ref} | Tipo: {doc.Documento} | Série: {doc.SerieFiscal} | Entidade: {doc.Entidade} | Venc: {doc.DataVenc:yyyy-MM-dd}");
                        Notas.Log($"CondPag: {doc.CondPag}, ModPag: {doc.ModPag}, Assinatura: {doc.DadosAssinatura}, TotalMerc: {doc.TotalMerc}, TotalIva: {doc.TotalIva}, NIF: {doc.NumContribuinte}, Nome: {doc.Nome}");

                        // Buscar sempre o Código de pagamento associado ao documento
                        string codigoPagamento = null;
                        string selectCodigoQuery = @"
    SELECT TOP 1 Codigo 
    FROM protelsevero.proteluser.V_CBLLinhasPagDocumentos 
    WHERE cab_ref = @ref";

                        using (var codigoCmd = new SqlCommand(selectCodigoQuery, connection))
                        {
                            codigoCmd.Parameters.AddWithValue("@ref", doc.Ref);
                            var codigoResult = await codigoCmd.ExecuteScalarAsync();
                            if (codigoResult != null && codigoResult != DBNull.Value)
                            {
                                codigoPagamento = codigoResult.ToString();
                            }
                        }

                        if (string.IsNullOrWhiteSpace(codigoPagamento))
                        {
                            Notas.Log($"⚠ Documento {doc.Ref} não tem Código de pagamento em V_CBLLinhasPagDocumentos.");
                            codigoPagamento = ""; // ou lançar erro, conforme necessidade
                        }

                        Notas.Log($"💳 Código de pagamento encontrado para {doc.Ref}: {codigoPagamento}");

                        var selectLinhasQuery = @"
                SELECT Referencia, quantidade, designacao, preco_unit, cIVA, IVA, ValorIVA, CodCentroCusto
                FROM protelsevero.proteluser.V_CBLLinhasDocumentos
                WHERE cab_ref = @ref";

                        var linhasCommand = new SqlCommand(selectLinhasQuery, connection);
                        linhasCommand.Parameters.Clear();
                        linhasCommand.Parameters.AddWithValue("@ref", doc.Ref);

                        var linhas = new List<object>();
                        using var linhasReader = await linhasCommand.ExecuteReaderAsync();
                        while (await linhasReader.ReadAsync())
                        {
                            decimal quantidade = 0;
                            if (decimal.TryParse(linhasReader["quantidade"].ToString(), out var parsedQty))
                            {
                                quantidade = Math.Abs(parsedQty);
                            }

                            decimal precUnit = 0;
                            decimal.TryParse(
                                linhasReader["preco_unit"].ToString().Replace(',', '.'),
                                NumberStyles.Any,
                                CultureInfo.InvariantCulture,
                                out precUnit
                            );

                            // Se for nota de crédito (NC), garante que o preço seja positivo
                            if (doc.SAFTPMS == "NC" && precUnit < 0)
                            {
                                precUnit = Math.Abs(precUnit);
                            }
                            else
                            {
                                if (precUnit < 0)
                                {
                                    // Para outros docs: só se preço for negativo → torna positivo
                                    // e quantidade passa a negativa
                                    precUnit = Math.Abs(precUnit);
                                    quantidade = -quantidade;
                                }
                                // Se o preço já for positivo, não altera nada
                            }
                            // Se o preço unitário for negativo, a quantidade também fica negativa
                            // if (precUnit < 0)
                            // {
                            //     quantidade = -quantidade;
                            // }

                            linhas.Add(new
                            {
                                Artigo = linhasReader["Referencia"].ToString(),
                                Descricao = linhasReader["designacao"].ToString(),
                                PrecUnit = precUnit,
                                Quantidade = quantidade,
                                Unidade = "UN",
                                CodIva = Convert.ToInt32(linhasReader["cIVA"]).ToString("D2"),
                                TaxaIva = linhasReader["IVA"],
                                TotalIva = linhasReader["ValorIVA"],
                                CCustoCBL = linhasReader["CodCentroCusto"].ToString()
                            });
                        }
                        linhasReader.Close();

                        Notas.Log($"Documento ref {doc.Ref} contém {linhas.Count} linha(s) carregada(s).");

                        foreach (var linha in linhas)
                        {
                            Notas.Log($" - Linha: {JsonSerializer.Serialize(linha)}");
                        }

                        if (!linhas.Any())
                        {
                            Notas.Log($"Sem linhas para o documento ref: {doc.Ref}, pulando.");
                            continue;
                        }

                        string ExtrairNumeroCertificado(string assinatura)
                        {
                            if (string.IsNullOrWhiteSpace(assinatura))
                                return "";

                            // Regex: pega os dígitos antes de "/ AT"
                            var match = Regex.Match(assinatura, @"(\d+)\s*/\s*AT");
                            return match.Success ? match.Groups[1].Value : "";
                        }

                        var documento = new Dictionary<string, object?>
                        {
                            ["linhas"] = linhas,
                            ["referencia"] = doc.Documento,
                            ["Documento"] = doc.Documento,
                            ["Requisicao"] = doc.Documento,
                            ["contaCBL"] = "",
                            ["nome"] = doc.Nome,
                            ["morada"] = doc.Morada,
                            ["pais"] = doc.Pais,
                            ["codigoPostal"] = doc.CodigoPostal,
                            ["localidade"] = doc.Localidade,
                            ["distrito"] = "",
                            ["numContribuinte"] = doc.NumContribuinte,
                            ["tipodoc"] = doc.SAFT,
                            ["refTipoDocOrig"] = doc.SAFTPMS,
                            ["serie"] = doc.SerieFiscal.ToString(),
                            ["refSerieDocOrig"] = doc.SerieFiscalPMS,
                            ["refDocOrig"] = doc.Numero,
                            ["entidade"] = "VD",
                            ["tipoEntidade"] = "C",
                            ["TotalMerc"] = Math.Abs(doc.TotalMerc),
                            ["TotalIva"] = Math.Abs(doc.TotalIva),
                            ["condPag"] = doc.CondPag,
                            ["modoPag"] = codigoPagamento,
                            ["assinatura"] = doc.Hash,
                            ["VersaoAssinatura"] = 1,
                            ["DocumentoCertificado"] = true,
                            ["Certificado"] = ExtrairNumeroCertificado(doc.DadosAssinatura),
                            ["horaDefinida"] = true,
                            ["dataDoc"] = doc.DataVenc.ToString("yyyy-MM-dd"),
                            ["dataVenc"] = DateTime.Now.ToString("yyyy-MM-dd")
                        };

                        // só adiciona se for NC
                        if (doc.SAFTPMS == "NC")
                        {
                            documento["motivoEstorno"] = "002";
                            documento["motivoEmissao"] = "002";

                        }

                        string jsonBody = JsonSerializer.Serialize(documento, new JsonSerializerOptions
                        {
                            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                        });

                        Notas.Log($"JSON enviado para documento ref {doc.Ref}:\n{jsonBody}");

                        var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

                        Notas.Log($"Enviando documento ref {doc.Ref}...");

                        var response = await client.PostAsync(url, content);
                        string apiResponse = await response.Content.ReadAsStringAsync();

                        int httpStatus = (int)response.StatusCode; // pega status HTTP

                        // Tenta extrair o StatusCode do JSON da API
                        int apiStatus = 0;
                        try
                        {
                            using var docJson = JsonDocument.Parse(apiResponse);
                            if (docJson.RootElement.TryGetProperty("StatusCode", out var statusProp))
                            {
                                apiStatus = statusProp.GetInt32();
                            }
                        }
                        catch
                        {
                            // se a resposta não for JSON válido, ignora
                        }

                        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            Notas.Log($"Token expirado ao enviar ref {doc.Ref}, vai reautenticar e tentar novamente.");
                            documentosAFalhar.Add(doc);
                            break;
                        }

                        if (response.IsSuccessStatusCode)
                        {
                            Notas.Log($"Documento ref {doc.Ref} criado com sucesso.");
                        }
                        else
                        {
                            Notas.Log($"Erro ao criar documento ref {doc.Ref}: {response.StatusCode}");
                        }

                        Notas.Log($"Resposta da API (ref {doc.Ref}): {apiResponse}");

                        // >>> INSERIR LOG NA TABELA
                        string insertLogQuery = @"
        INSERT INTO dbo.CBLLogs (docID, requestBody, status, responseBody, ref)
        VALUES (@docID, @requestBody, @status, @responseBody, @ref)
    ";

                        using (var insertCmd = new SqlCommand(insertLogQuery, connection))
                        {
                            insertCmd.Parameters.AddWithValue("@docID", doc.Numero);
                            insertCmd.Parameters.AddWithValue("@requestBody", jsonBody ?? (object)DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@status", apiStatus != 0 ? apiStatus : httpStatus);
                            insertCmd.Parameters.AddWithValue("@responseBody", apiResponse ?? (object)DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@ref", doc.Ref ?? (object)DBNull.Value);

                            await insertCmd.ExecuteNonQueryAsync();
                        }
                        if (!response.IsSuccessStatusCode)
                        {
                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    string errorMsg = $"Exceção ao criar documentos: {ex.Message}";
                    Console.WriteLine(errorMsg);
                    Notas.Log(errorMsg);
                }
            }

            private async Task ReenviarDocumentoAsync(int refDoc)
            {
                try
                {
                    using var connection = new SqlConnection(_config.ConnectionString);
                    await connection.OpenAsync();

                    // 1️⃣ Pegar intervalo do config
                    var resendFrom = DateTime.Parse(_config.ResendFrom);
                    var resendTo = DateTime.Parse(_config.ResendTo);

                    // 2️⃣ Buscar o documento original entre as datas
                    string query = @"
            SELECT ref, documento, SAFT, SAFTPMS, SerieFiscal, SerieFiscalPMS, entidade, data, DadosAssinatura, CondPag, ModPag, TotalLiq, TotIVA, nif, nome,
                   morada, codigopostal, localidade, distrito, pais, Numero, Hash
            FROM protelsevero.proteluser.V_CBLCabDocumentos
            WHERE ref = @ref";

                    using var cmd = new SqlCommand(query, connection);
                    cmd.Parameters.AddWithValue("@ref", refDoc);

                    using var reader = await cmd.ExecuteReaderAsync();
                    if (!await reader.ReadAsync())
                    {
                        Notas.Log($"⚠ Documento {refDoc} não encontrado.");
                        return;
                    }

                    // 3️⃣ Montar o objeto de envio novamente (dados originais)
                    var doc = new
                    {
                        Ref = reader["ref"].ToString(),
                        Documento = reader["documento"].ToString(),
                        SAFT = reader["SAFT"].ToString(),
                        SAFTPMS = reader["SAFTPMS"].ToString(),
                        SerieFiscal = reader["SerieFiscal"].ToString(),
                        SerieFiscalPMS = reader["SerieFiscalPMS"].ToString(),
                        Entidade = reader["entidade"].ToString(),
                        DataVenc = ((DateTimeOffset)reader["data"]).DateTime,
                        DadosAssinatura = reader["DadosAssinatura"].ToString(),
                        CondPag = reader["CondPag"].ToString(),
                        ModPag = reader["ModPag"].ToString(),
                        TotalMerc = reader["TotalLiq"] != DBNull.Value ? Convert.ToDouble(reader["TotalLiq"]) : 0,
                        TotalIva = reader["TotIVA"] != DBNull.Value ? Convert.ToDouble(reader["TotIVA"]) : 0,
                        NumContribuinte = Regex.Replace(reader["nif"].ToString(), @"[^\d]", ""),
                        Nome = reader["nome"].ToString(),
                        Morada = reader["morada"].ToString(),
                        CodigoPostal = reader["codigopostal"].ToString(),
                        Localidade = reader["localidade"].ToString(),
                        Distrito = reader["distrito"].ToString(),
                        Pais = reader["pais"].ToString(),
                        Numero = Convert.ToInt32(reader["Numero"]),
                        Hash = reader["Hash"].ToString()
                    };
                    reader.Close();

                    //                 // ✅ Buscar o Codigo de pagamento
                    //                 string selectCodigoPag = @"
                    // SELECT TOP 1 Codigo
                    // FROM protelsevero.proteluser.V_CBLLinhasPagDocumentos
                    // WHERE cab_ref = @refDoc";

                    //                 using var cmdCodigo = new SqlCommand(selectCodigoPag, connection);
                    //                 cmdCodigo.Parameters.AddWithValue("@refDoc", doc.Ref);

                    //                 var codigo = (string?)await cmdCodigo.ExecuteScalarAsync() ?? "";

                    // Buscar sempre o Código de pagamento associado ao documento
                    string codigo = null;
                    string selectCodigoQuery = @"
                        SELECT TOP 1 Codigo 
                        FROM protelsevero.proteluser.V_CBLLinhasPagDocumentos
                        WHERE cab_ref = @ref";

                    using (var codigoCmd = new SqlCommand(selectCodigoQuery, connection))
                    {
                        codigoCmd.Parameters.AddWithValue("@ref", doc.Ref);
                        var codigoResult = await codigoCmd.ExecuteScalarAsync();
                        if (codigoResult != null && codigoResult != DBNull.Value)
                        {
                            codigo = codigoResult.ToString();
                        }
                    }

                    if (string.IsNullOrWhiteSpace(codigo))
                    {
                        Notas.Log($"⚠ Documento {doc.Ref} não tem Código de pagamento em V_CBLLinhasPagDocumentos.");
                        codigo = ""; // ou lançar erro, conforme necessidade
                    }

                    Notas.Log($"💳 Código de pagamento encontrado para {doc.Ref}: {codigo}");

                    // 4️⃣ Buscar as linhas do documento original
                    var linhas = new List<object>();
                    string selectLinhas = @"
            SELECT Referencia, quantidade, designacao, preco_unit, cIVA, IVA, ValorIVA, CodCentroCusto
            FROM protelsevero.proteluser.V_CBLLinhasDocumentos
            WHERE cab_ref = @ref";

                    using var cmdLinhas = new SqlCommand(selectLinhas, connection);
                    cmdLinhas.Parameters.AddWithValue("@ref", doc.Ref);

                    using var linhasReader = await cmdLinhas.ExecuteReaderAsync();
                    while (await linhasReader.ReadAsync())
                    {
                        decimal quantidade = Math.Abs(Convert.ToDecimal(linhasReader["quantidade"]));
                        decimal precoUnit = Convert.ToDecimal(linhasReader["preco_unit"], CultureInfo.InvariantCulture);

                        // Se for nota de crédito (NC), garante que o preço seja positivo
                        if (doc.SAFTPMS == "NC" && precoUnit < 0)
                        {
                            precoUnit = Math.Abs(precoUnit);
                        }
                        else
                        {
                            if (precoUnit < 0)
                            {
                                // Para outros docs: só se preço for negativo → torna positivo
                                // e quantidade passa a negativa
                                precoUnit = Math.Abs(precoUnit);
                                quantidade = -quantidade;
                            }
                            // Se o preço já for positivo, não altera nada
                        }
                        // Se o preço unitário for negativo, a quantidade também fica negativa
                        // if (precoUnit < 0)
                        // {
                        //     quantidade = -quantidade;
                        // }
                        linhas.Add(new
                        {
                            Artigo = linhasReader["Referencia"].ToString(),
                            Descricao = linhasReader["designacao"].ToString(),
                            PrecUnit = precoUnit,
                            Quantidade = quantidade,
                            Unidade = "UN",
                            CodIva = Convert.ToInt32(linhasReader["cIVA"]).ToString("D2"),
                            TaxaIva = linhasReader["IVA"],
                            TotalIva = linhasReader["ValorIVA"],
                            CCustoCBL = linhasReader["CodCentroCusto"].ToString()
                        });
                    }
                    linhasReader.Close();

                    if (!linhas.Any())
                    {
                        Notas.Log($"⚠ Documento {refDoc} sem linhas, não foi reenviado.");
                        return;
                    }

                    string ExtrairNumeroCertificado(string assinatura)
                    {
                        if (string.IsNullOrWhiteSpace(assinatura))
                            return "";

                        // Regex: pega os dígitos antes de "/ AT"
                        var match = Regex.Match(assinatura, @"(\d+)\s*/\s*AT");
                        return match.Success ? match.Groups[1].Value : "";
                    }

                    // 5️⃣ Montar o JSON para enviar
                    var documento = new Dictionary<string, object?>
                    {
                        ["linhas"] = linhas,
                        ["referencia"] = doc.Documento,
                        ["Documento"] = doc.Documento,
                        ["Requisicao"] = doc.Documento,
                        ["contaCBL"] = "",
                        ["nome"] = doc.Nome,
                        ["morada"] = doc.Morada,
                        ["pais"] = doc.Pais,
                        ["codigoPostal"] = doc.CodigoPostal,
                        ["localidade"] = doc.Localidade,
                        ["distrito"] = "",
                        ["numContribuinte"] = doc.NumContribuinte,
                        ["tipodoc"] = doc.SAFT,
                        ["refTipoDocOrig"] = doc.SAFTPMS,
                        ["serie"] = doc.SerieFiscal,
                        ["refSerieDocOrig"] = doc.SerieFiscalPMS,
                        ["refDocOrig"] = doc.Numero,
                        ["entidade"] = "VD",
                        ["tipoEntidade"] = "C",
                        ["TotalMerc"] = Math.Abs(doc.TotalMerc),
                        ["TotalIva"] = Math.Abs(doc.TotalIva),
                        ["condPag"] = doc.CondPag,
                        ["modoPag"] = codigo,
                        ["assinatura"] = doc.Hash,
                        ["VersaoAssinatura"] = 1,
                        ["DocumentoCertificado"] = true,
                        ["Certificado"] = ExtrairNumeroCertificado(doc.DadosAssinatura),
                        ["horaDefinida"] = true,
                        ["dataDoc"] = doc.DataVenc.ToString("yyyy-MM-dd"),
                        ["dataVenc"] = DateTime.Now.ToString("yyyy-MM-dd")
                    };

                    // só adiciona se for NC
                    if (doc.SAFTPMS == "NC")
                    {
                        documento["motivoEstorno"] = "002";
                        documento["motivoEmissao"] = "002";
                    }


                    string jsonBody = JsonSerializer.Serialize(documento, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    });

                    // 6️⃣ Enviar para a API
                    using var client = new HttpClient();
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _config.Authorization.secondToken);

                    string url = $"{_config.ApiUrl}/Palacete/Vendas/";
                    var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
                    var response = await client.PostAsync(url, content);

                    string apiResponse = await response.Content.ReadAsStringAsync();
                    int httpStatus = (int)response.StatusCode;

                    Notas.Log($"🔄 Reenvio DocID {refDoc}: {httpStatus} | Resposta: {apiResponse}");

                    // 7️⃣ Atualizar o log com o novo estado e json
                    string updateLog = @"
            UPDATE dbo.CBLLogs 
            SET requestBody = @requestBody, status = @status, responseBody = @responseBody, createdAt = GETDATE()
            WHERE ref = @ref";

                    using var updateCmd = new SqlCommand(updateLog, connection);
                    updateCmd.Parameters.AddWithValue("@ref", refDoc);
                    updateCmd.Parameters.AddWithValue("@requestBody", jsonBody);
                    updateCmd.Parameters.AddWithValue("@status", httpStatus);
                    updateCmd.Parameters.AddWithValue("@responseBody", apiResponse);

                    await updateCmd.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    Notas.Log($"❌ Erro ao reenviar documento {refDoc}: {ex.Message}");
                }
            }



        }
    }
}
