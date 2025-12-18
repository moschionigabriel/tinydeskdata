(function() {
	var tinyDeskData = {
		pipe : (
			function() {

				function _moveGetData(obj) {
					let data;
					
					if (obj.source.where == 'drive') {
						let file_id = obj.source.config.file_id

						let file, sheet_name, sheet
						
						if (Drive.Files.get(file_id).mimeType == 'application/vnd.google-apps.spreadsheet'){
						file = SpreadsheetApp.openById(file_id)
						if (obj.source.config.sheet_name) {sheet_name = obj.source.config.sheet_name} else {sheet_name = file.getSheets()[0].getName()}
						sheet = file.getSheetByName(sheet_name)
						data = sheet.getDataRange().getDisplayValues()
						return data
						}

						else if (Drive.Files.get(file_id).mimeType == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'){
						file = DriveApp.getFileById(file_id)
						if (obj.source.config.sheet_name) {sheet_name = obj.source.config.sheet_name} else {sheet_name = file_temp.getSheets()[0].getName()}
						let file_blob = file.getBlob()
						let config = {
							title: "temp_tdp",
							mimeType: MimeType.GOOGLE_SHEETS
						}
						let temp_file = Drive.Files.create(config, file_blob)
						let temp_file_id = temp_file.id
						let file_temp = SpreadsheetApp.openById(temp_file_id)
						sheet = file_temp.getSheetByName(sheet_name)
						data = sheet.getDataRange().getDisplayValues()
						Drive.Files.remove(temp_file_id)
						return data
						}

						else if (Drive.Files.get(file_id).mimeType == 'text/csv'){
						file = DriveApp.getFileById(file_id)
						data = Utilities.parseCsv(file.getBlob().getDataAsString("utf-8"))
						return data
						} 
					} else if (obj.source.where == 'here') {
						
						let parent_folder = (obj.parent_folder) ? obj.parent_folder + '/' : ''
          				let file_name = parent_folder + obj.source.config.file_name
						let file_extension = file_name.match(/\.(.*)/)[1]
						let platform = obj.source.config.platform

						if (file_extension == 'sql' && platform == 'bigquery') {
						let bq_project_name = obj.source.config.credentials.project_name
						let query_string = HtmlService.createHtmlOutputFromFile(file_name).getContent().toString().replace(/\n/g,'')
						let request = {query: query_string,useLegacySql: false}
						let query_result = BigQuery.Jobs.query(request, bq_project_name)
						let job_id = query_result.jobReference.jobId
						let sleepTimeMs = 500
						while (!query_result.jobComplete) {Utilities.sleep(sleepTimeMs); queryResults = BigQuery.Jobs.getQueryResults(bq_project_name, job_id)}
						let data = query_result.rows.map(row => row.f.map(cell => cell.v))
						let fields = query_result.schema.fields;
						let header = fields.map(field => field.name)
						data.unshift(header)
						return data
						} else if (file_extension == 'gs') {
						let result = eval(HtmlService.createHtmlOutputFromFile(file_name).getContent().toString().replace(/\n/g,''))
						data = result
						return data 
						}

					} else if (obj.source.where == 'sql_platform') {
						let bq_project_name = obj.source.config.credentials.project_name
						let query_string = 'select * from ' + obj.source.config.schema_name + '.' + obj.source.config.table_name;
						let request = {query: query_string,useLegacySql: false}
						let query_result = BigQuery.Jobs.query(request, bq_project_name)
						let job_id = query_result.jobReference.jobId
						let sleepTimeMs = 500
						while (!query_result.jobComplete) {Utilities.sleep(sleepTimeMs); queryResults = BigQuery.Jobs.getQueryResults(bq_project_name, job_id)}
						let data = query_result.rows.map(row => row.f.map(cell => cell.v))
						let fields = query_result.schema.fields;
						let header = fields.map(field => field.name)
						data.unshift(header)
						return data
					}

					return data
				
				}
				function _moveLoadData(obj,data) {
					let num_rows = data.length
					let num_columns = data[0].length
						
					if (obj.destination.where == 'drive') {
						if (obj.destination.config.file_type == 'sheets') {
						if (obj.destination.config.new_file_flag == false) {
							let existing_file_id, existing_file, existing_sheet_name, existing_sheet; 
							if (obj.destination.config.file_id) {existing_file_id = obj.destination.config.file_id} else {existing_file_id = DriveApp.getFilesByName(obj.destination.config.file_name).next().getId()}
							existing_file = SpreadsheetApp.openById(existing_file_id)
							if (obj.destination.config.sheet_name) {existing_sheet_name = obj.destination.config.sheet_name} else {existing_sheet_name = existing_file.getSheets()[0].getName()}
							existing_sheet = existing_file.getSheetByName(existing_sheet_name)
							let existing_target_range
							if (obj.destination.config.write_disposition == 'append') {
							existing_target_range = existing_sheet.getRange(existing_sheet.getLastRow() + 1,1,num_rows,num_columns);
							existing_target_range.setValues(data);
							SpreadsheetApp.flush()
							//console.log('foi')
							
							} else if (obj.destination.config.write_disposition == 'truncate') {
							let existing_data_range = existing_sheet.getDataRange();
							existing_data_range.clearContent();
							existing_target_range = existing_sheet.getRange(1, 1, num_rows, num_columns);
							existing_target_range.setValues(data);
							SpreadsheetApp.flush()
							//console.log('foi')
							}
						} else {
							let file_id, file,file_ss, sheet_name, sheet; 
							let config = {
							name: obj.destination.config.file_name,
							mimeType: MimeType.GOOGLE_SHEETS
							}
							file = Drive.Files.create(config)
							file_id = file.id
							file_ss = SpreadsheetApp.openById(file_id)
							if (obj.destination.config.sheet_name) {sheet_name = obj.destination.config.sheet_name} else {sheet_name = file_ss.getSheets()[0].getName()}
							sheet = file_ss.getSheetByName(sheet_name)
							sheet.getRange(1, 1, num_rows, num_columns).setValues(data)
							SpreadsheetApp.flush()
							if (obj.destination.config.folder_id) {let resource;Drive.Files.update(resource,file_id,null,{addParents: obj.destination.config.folder_id,removeParents: file.parents});
							}
						}
						} else if (obj.destination.config.file_type == 'xlsx') {
						if (obj.destination.config.new_file_flag == false) {
							let source_file_id, source_file,source_file_blob,source_file_name,temp_file,temp_file_id,file_temp,temp_file_blob,temp_sheet,sheet_name,target_file, target_file_id ,folder_id,folder;
							if (obj.destination.config.file_id) {source_file_id = obj.destination.config.file_id} else {source_file_id = DriveApp.getFilesByName(obj.destination.config.file_name).next().getId()}
							source_file = DriveApp.getFileById(source_file_id)
							source_file_blob = source_file.getBlob()
							source_file_name  = source_file.getName()
							let config = {
							name: source_file_name,
							mimeType: MimeType.GOOGLE_SHEETS
							}

							temp_file = Drive.Files.create(config, source_file_blob)
							temp_file_id = temp_file.id
							file_temp = SpreadsheetApp.openById(temp_file_id)
							if (obj.source.config.sheet_name) {sheet_name = obj.source.config.sheet_name} else {sheet_name = file_temp.getSheets()[0].getName()}
							temp_sheet = file_temp.getSheetByName(sheet_name)

							let temp_target_range
							if (obj.destination.config.write_disposition == 'append') {
							temp_target_range = temp_sheet.getRange(temp_sheet.getLastRow() + 1,1,num_rows,num_columns);
							temp_target_range.setValues(data);
							SpreadsheetApp.flush()
							} else if (obj.destination.config.write_disposition == 'truncate') {
							let temp_target_range = temp_sheet.getDataRange();
							temp_target_range.clearContent();
							temp_target_range = temp_sheet.getRange(1, 1, num_rows, num_columns);
							temp_target_range.setValues(data);
							SpreadsheetApp.flush()
							}
							
							
							

							var url = 'https://docs.google.com/spreadsheets/d/' + temp_file_id + '/export?format=xlsx';
							var token = ScriptApp.getOAuthToken();
							
							var response = UrlFetchApp.fetch(url, {
							headers: {
								'Authorization': 'Bearer ' + token
							}
							});
							
							temp_file_blob = response.getBlob().setName(source_file_name);
							if (obj.destination.config.folder_id) {folder_id = obj.destination.config.folder_id} else {folder_id = source_file.getParents().next().getId()}
							folder = DriveApp.getFolderById(folder_id);
							target_file = folder.createFile(temp_file_blob);
							

							//target_file = Drive.Files.copy({name: source_file_name, mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}, temp_file_id);
							//target_file_id = target_file.id
							//if (obj.destination.config.folder_id) {folder_id = obj.destination.config.folder_id} else {folder_id = source_file.getParents().next().getId()}
							//let resource;Drive.Files.update(resource,target_file_id,null,{addParents: folder_id,removeParents: target_file.parents});
							Drive.Files.remove(source_file_id)
							Drive.Files.remove(temp_file_id)
						} else {
							let temp_file_id, temp_file,temp_file_ss, temp_sheet,target_file_name; 
							let config = {
							name: obj.destination.config.file_name,
							mimeType: MimeType.GOOGLE_SHEETS
							}
							temp_file = Drive.Files.create(config)
							temp_file_id = temp_file.id
							temp_file_ss = SpreadsheetApp.openById(temp_file_id)
							temp_sheet = temp_file_ss.getSheets()[0]
							if (obj.destination.config.sheet_name) {temp_sheet.setName(obj.destination.config.sheet_name)}
							temp_sheet.getRange(1, 1, num_rows, num_columns).setValues(data)
							SpreadsheetApp.flush()
							if (obj.destination.config.folder_id) {let resource;Drive.Files.update(resource,temp_file_id,null,{addParents: obj.destination.config.folder_id,removeParents: temp_file.parents});
							var url = 'https://docs.google.com/spreadsheets/d/' + temp_file_id + '/export?format=xlsx';
							var token = ScriptApp.getOAuthToken();
							
							var response = UrlFetchApp.fetch(url, {
							headers: {
								'Authorization': 'Bearer ' + token
							}
							});

							if (obj.destination.config.file_name.endsWith('.xlsx')) {target_file_name = obj.destination.config.file_name} else (target_file_name = obj.destination.config.file_name + '.xlsx')
							
							temp_file_blob = response.getBlob().setName(target_file_name);
							if (obj.destination.config.folder_id) {folder_id = obj.destination.config.folder_id} else {folder_id = DriveApp.getRootFolder().getId()}
							folder = DriveApp.getFolderById(folder_id);
							target_file = folder.createFile(temp_file_blob);
							
							Drive.Files.remove(temp_file_id)
							}
						}
						} else if (obj.destination.config.file_type == 'csv') {
						if (obj.destination.config.new_file_flag == false) {
							let source_file_id, source_file,source_file_blob, source_data, new_data, target_data, target_blob
							if (obj.destination.config.file_id) {source_file_id = obj.destination.config.file_id} else {source_file_id = DriveApp.getFilesByName(obj.destination.config.file_name).next().getId()}
							source_file = DriveApp.getFileById(source_file_id)
							source_file_blob = source_file.getBlob()
							source_file_name  = source_file.getName()
							source_data = source_file_blob.getDataAsString()
							if (source_data.charCodeAt(0) === 0xFEFF) {source_data = source_data.substring(1)}
							new_data = data.map(row => 
							row.map(cell => {
								const cellStr = String(cell);
								if (cellStr.includes(',') || cellStr.includes('"') || cellStr.includes('\n')) {
								return '"' + cellStr.replace(/"/g, '""') + '"';
								}
								return cellStr;
							}).join(',')
							).join('\n');
							if (obj.destination.config.write_disposition == 'append') {
							target_data = source_data.trimEnd() + '\n' + new_data;
							} else if (obj.destination.config.write_disposition == 'truncate') {
							target_data = new_data;
							}
							target_blob = Utilities.newBlob('\ufeff' + target_data, 'text/csv', source_file_name);
							source_file.setContent(target_blob.getDataAsString());
						} else {
							let target_data, target_blob, target_file_name, folder, fodler_id
							target_data = data.map(row => 
							row.map(cell => {
								const cellStr = String(cell);
								if (cellStr.includes(',') || cellStr.includes('"') || cellStr.includes('\n')) {
								return '"' + cellStr.replace(/"/g, '""') + '"';
								}
								return cellStr;
							}).join(',')
							).join('\n');
							target_file_name = obj.destination.config.file_name
							if (obj.destination.config.file_name.endsWith('.csv')) {target_file_name = obj.destination.config.file_name} else (target_file_name = obj.destination.config.file_name + '.csv')
							target_blob = Utilities.newBlob('\ufeff' + target_data, 'text/csv', target_file_name);
							if (obj.destination.config.folder_id) {folder_id = obj.destination.config.folder_id} else {folder_id = DriveApp.getRootFolder().getId()}
							folder = DriveApp.getFolderById(folder_id);
							target_file = folder.createFile(target_blob);
						}
						}
					} else if (obj.destination.where == 'sql_platform') {
						if (obj.destination.config.platform == 'bigquery') {
							let bq_project_name, schema_name, table_name, write_disposition
							bq_project_name = obj.destination.config.credentials.project_name
							schema_name = obj.destination.config.schema_name
							table_name = obj.destination.config.table_name
							if(obj.destination.config.write_disposition) {write_disposition = obj.destination.config.write_disposition} else {write_disposition = 'append'}

							let headers = data[0];
							let rows = data.slice(1);
							
							let table_schema = {
							fields: headers.map(header => ({
								name: String(header).replace(/[^a-zA-Z0-9_]/g, '_'), 
								type: 'STRING'
							}))
							};

							let jsonRows = rows.map(row => {
							let rowObject = {};
							headers.forEach((header, index) => {
								let cleanHeader = String(header).replace(/[^a-zA-Z0-9_]/g, '_');
								rowObject[cleanHeader] = row[index] !== null && row[index] !== undefined ? String(row[index]) : null;
							});
							return rowObject;
							});

							let job = {
							configuration: {
								load: {
								destinationTable: {
									projectId: bq_project_name,
									datasetId: schema_name,
									tableId: table_name
								},
								schema: table_schema,
								sourceFormat: 'NEWLINE_DELIMITED_JSON',
								writeDisposition: 'WRITE_' + write_disposition.toUpperCase(), 
								autodetect: false
								}
							}
							}

							let ndjson = jsonRows.map(row => JSON.stringify(row)).join('\n');
							let blob = Utilities.newBlob(ndjson, 'application/octet-stream');
							

							try {
							const insertJob = BigQuery.Jobs.insert(job, bq_project_name, blob);
							
							//Logger.log('Job ID: ' + insertJob.jobReference.jobId);
							
							let jobStatus = BigQuery.Jobs.get(bq_project_name, insertJob.jobReference.jobId);
							while (jobStatus.status.state !== 'DONE') {
								Utilities.sleep(1000);
								jobStatus = BigQuery.Jobs.get(bq_project_name, insertJob.jobReference.jobId);
							}
							
							if (jobStatus.status.errorResult) {
								throw new Error('Erro no BigQuery: ' + JSON.stringify(jobStatus.status.errorResult));
							}
							
							//Logger.log('Dados inseridos com sucesso na tabela: ' + table_name);
							return jobStatus;
							}  catch (error) {
							//Logger.log('Erro ao inserir dados: ' + error.toString());
							throw error;
							}
						}
					}
				}

				function _pipeline(obj, ...functions) {return functions.reduce((result, fn) => fn(result), obj);}
				function _transformGetRawCode(obj) {
					let parent_folder = (obj.parent_folder) ? obj.parent_folder + '/' : ''
					for(model of obj.models) {
						let file_name = parent_folder + model.name + '.sql' + '.html'
						model.raw_code = HtmlService.createHtmlOutputFromFile(file_name).getContent().toString().replace(/\n/g,'')
					}
					return obj
				}
				function _transformSetDependencies(obj) {
					for (model of obj.models) {
						let regex = /{{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*}}/g;
						let dependencies = [];
						let match;
						while ((match = regex.exec(model.raw_code)) !== null) {
						dependencies.push(match[1]);
						}

						model.depends_on = dependencies
					}
					//console.log(obj)
					return obj
				}
				function _transformSort(obj) {
					function topologicalSortKahn(items, nameKey, dependsOnKey) {
						// 1. Inicialização da Estrutura de Grafo
						const graph = {}; // { 'ObjetoA': ['ObjetoB', 'ObjetoC'], ... } - Mapeia Objeto -> Sucessores
						const inDegree = {}; // { 'ObjetoA': 0, 'ObjetoB': 1, ... } - Mapeia Objeto -> Grau de Entrada (número de dependências)
						const itemMap = {}; // Mapeia o nome do objeto para o objeto completo

						for (const item of items) {
						const name = item[nameKey];
						graph[name] = [];
						inDegree[name] = 0;
						itemMap[name] = item;
						}

						// 2. Construção do Grafo e Cálculo do Grau de Entrada
						for (const item of items) {
						const dependentName = item[nameKey];
						const dependencies = item[dependsOnKey] || []; // Pega a lista de dependências

						for (const dependencyName of dependencies) {
							if (graph[dependencyName] && inDegree.hasOwnProperty(dependentName)) {
							// A aresta vai da dependência (pré-requisito) para o objeto dependente.
							// Se A depende de B, a aresta é B -> A.
							graph[dependencyName].push(dependentName);
							inDegree[dependentName]++;
							} else if (!itemMap.hasOwnProperty(dependencyName)) {
							//Logger.log(`Atenção: Dependência "${dependencyName}" não encontrada na lista de itens.`);
							}
						}
						}
						
						// 

						// 3. Inicialização da Fila (Queue) com Nós de Grau de Entrada Zero (Fontes)
						const queue = [];
						for (const name in inDegree) {
						if (inDegree[name] === 0) {
							queue.push(name);
						}
						}

						// 4. Processamento da Fila
						const sortedNames = [];
						while (queue.length > 0) {
						// Retira o próximo item sem dependências não resolvidas
						const uName = queue.shift();
						sortedNames.push(uName);

						// Para cada vizinho (sucessor) de 'u'
						for (const vName of graph[uName]) {
							// "Remove" a aresta, decrementando o grau de entrada do vizinho
							inDegree[vName]--;

							// Se o vizinho agora tem grau de entrada zero, ele se torna uma nova "fonte"
							if (inDegree[vName] === 0) {
							queue.push(vName);
							}
						}
						}
						
						// 5. Verificação de Ciclo
						if (sortedNames.length !== items.length) {
						// Se o número de itens ordenados for menor que o total, há um ciclo.
						//Logger.log('ERRO: Ciclo de dependência detectado. Ordenação Topológica impossível.');
						return null; // Retorna null para indicar falha
						}

						// 6. Mapeamento final para Objetos
						return sortedNames.map(name => itemMap[name]);
					}

					obj.models = topologicalSortKahn(obj.models, "name", "depends_on")

					return obj;


				}
				function _transformCompile(obj) {
					function processJinjaTemplate(code) {
						let processedCode = code;
						
						// Regex para capturar {% set variavel = [array] %}
						const setRegex = /{%\s*set\s+(\w+)\s*=\s*\[([^\]]+)\]\s*%}/g;
						const variables = {};
						
						// Extrair variáveis definidas com {% set %}
						let setMatch;
						while ((setMatch = setRegex.exec(code)) !== null) {
						const varName = setMatch[1];
						const arrayContent = setMatch[2];
						// Parsear o array, removendo aspas e espaços
						variables[varName] = arrayContent.split(',').map(item => 
							item.trim().replace(/['"]/g, '')
						);
						}
						
						// Remover as declarações {% set %} do código
						processedCode = processedCode.replace(setRegex, '');
						
						// Processar loops {% for %}
						const forRegex = /{%\s*for\s+(\w+)\s+in\s+(\w+)\s*-%}([\s\S]*?){%\s*endfor\s*-%}/g;
						let forMatch;
						
						while ((forMatch = forRegex.exec(processedCode)) !== null) {
						const iteratorVar = forMatch[1]; // ex: payment_method
						const arrayVar = forMatch[2];    // ex: payment_methods
						const loopContent = forMatch[3]; // conteúdo do loop
						
						if (variables[arrayVar]) {
							// Mantém as vírgulas no template, então só junta com \n para preservar a vírgula que já tem
							let expandedContent = variables[arrayVar].map(item => {
							let iterationContent = loopContent;
							const varRegex = new RegExp(`{{\\s*${iteratorVar}\\s*}}`, 'g');
							iterationContent = iterationContent.replace(varRegex, item);
							return iterationContent.trim();  // NÃO remova vírgula aqui!
							}).join('\n');  // junta com quebra de linha, sem vírgula aqui
							
							processedCode = processedCode.replace(forMatch[0], expandedContent);
						}
						}
						
						return processedCode;
					}
					function processRefs(compiledCode, obj) {
						// Mapeia nome -> database.schema.name
						const nodeMap = {};
						for (const key in obj.models) {
						const node = obj.models[key];
						nodeMap[node.name] = `${obj.config.credentials.project_name}.${node.schema_name}.${node.name}`;
						}
						
						// Substitui refs no código compilado
						return compiledCode.replace(
						/\{\{\s*ref\((['"])(.*?)\1\)\s*\}\}/g,
						(match, quote, refName) => nodeMap[refName] || match
						);
					}


					for (model of obj.models) {
						
						if (model.raw_code) {

						let compiledCode = model.raw_code;
						
						// 1. Processar templates Jinja2
						compiledCode = processJinjaTemplate(compiledCode);
						
						// 2. Substituir referências {{ ref() }}
						compiledCode = processRefs(compiledCode, obj);

						model.compiled_code = compiledCode;
						}
						}
					
					return obj;
				}
				function _transformExecute(obj) {
					function runDDL(project,sql) {
						var queryResults = BigQuery.Jobs.query({query: sql, useLegacySql: false}, project);
						var jobId = queryResults.jobReference.jobId;

						// Espera o job completar
						while (BigQuery.Jobs.get(project, jobId).status.state !== 'DONE') {
						Utilities.sleep(2000);
						}

						var job = BigQuery.Jobs.get(project, jobId);
						if (job.status.errorResult) {
						return 'Erro: ' + job.status.errorResult.message;
						}
						return 'Comando executado com sucesso!';
					}

					for(model of obj.models) {
						let run_query;
						if (model.write_disposition == 'append') {
							if (model.partition_column) {
								run_query = `INSERT INTO ${obj.config.credentials.project_name}.${model.schema_name}.${model.name} (${model.compiled_code} PARTITION BY ${model.partition_column})  `
							} else {
								run_query = `INSERT INTO ${obj.config.credentials.project_name}.${model.schema_name}.${model.name} (${model.compiled_code})`
							}	
						} else {
							if (model.partition_column) {
								run_query = `CREATE OR REPLACE ${model.materialized.toUpperCase()} ${obj.config.credentials.project_name}.${model.schema_name}.${model.name} AS (${model.compiled_code} PARTITION BY ${model.partition_column}) `
							} else {
								run_query = `CREATE OR REPLACE ${model.materialized.toUpperCase()} ${obj.config.credentials.project_name}.${model.schema_name}.${model.name} AS (${model.compiled_code})`
							}
						}
						//console.log(run_query)
						runDDL(obj.config.credentials.project_name,run_query)
					}
					
					return obj
				}

				function _orchestrateCreateLog(obj) {
					let now = new Date();
					let timeZone = Session.getScriptTimeZone(); 
					let formattedDate = Utilities.formatDate(now, timeZone, "yyyy-MM-dd HH:mm:ss")
					obj.log = {}
					obj.log.name = ''
					obj.log.name = obj.name
					obj.log.start = formattedDate
					obj.log.nodes = []
					
					for(node of obj.nodes) {
					obj.log.nodes.push(node)
					}

					for(node of obj.log.nodes) {
					let source_check = (Array.isArray(node.info)) ? node.info[0] : node.info; 
					let type = (source_check.source) ? 'move' : 'transform';
					node.type = ''
					node.type = type
					}

					return obj
				
				}
				function _orchestrateSort(obj) {
					function topologicalSortKahn(items, nameKey, dependsOnKey) {
						// 1. Inicialização da Estrutura de Grafo
						const graph = {}; // { 'ObjetoA': ['ObjetoB', 'ObjetoC'], ... } - Mapeia Objeto -> Sucessores
						const inDegree = {}; // { 'ObjetoA': 0, 'ObjetoB': 1, ... } - Mapeia Objeto -> Grau de Entrada (número de dependências)
						const itemMap = {}; // Mapeia o nome do objeto para o objeto completo

						for (const item of items) {
						const name = item[nameKey];
						graph[name] = [];
						inDegree[name] = 0;
						itemMap[name] = item;
						}

						// 2. Construção do Grafo e Cálculo do Grau de Entrada
						for (const item of items) {
						const dependentName = item[nameKey];
						const dependencies = item[dependsOnKey] || []; // Pega a lista de dependências

						for (const dependencyName of dependencies) {
							if (graph[dependencyName] && inDegree.hasOwnProperty(dependentName)) {
							// A aresta vai da dependência (pré-requisito) para o objeto dependente.
							// Se A depende de B, a aresta é B -> A.
							graph[dependencyName].push(dependentName);
							inDegree[dependentName]++;
							} else if (!itemMap.hasOwnProperty(dependencyName)) {
							//Logger.log(`Atenção: Dependência "${dependencyName}" não encontrada na lista de itens.`);
							}
						}
						}
						
						// 

						// 3. Inicialização da Fila (Queue) com Nós de Grau de Entrada Zero (Fontes)
						const queue = [];
						for (const name in inDegree) {
						if (inDegree[name] === 0) {
							queue.push(name);
						}
						}

						// 4. Processamento da Fila
						const sortedNames = [];
						while (queue.length > 0) {
						// Retira o próximo item sem dependências não resolvidas
						const uName = queue.shift();
						sortedNames.push(uName);

						// Para cada vizinho (sucessor) de 'u'
						for (const vName of graph[uName]) {
							// "Remove" a aresta, decrementando o grau de entrada do vizinho
							inDegree[vName]--;

							// Se o vizinho agora tem grau de entrada zero, ele se torna uma nova "fonte"
							if (inDegree[vName] === 0) {
							queue.push(vName);
							}
						}
						}
						
						// 5. Verificação de Ciclo
						if (sortedNames.length !== items.length) {
						// Se o número de itens ordenados for menor que o total, há um ciclo.
						//Logger.log('ERRO: Ciclo de dependência detectado. Ordenação Topológica impossível.');
						return null; // Retorna null para indicar falha
						}

						// 6. Mapeamento final para Objetos
						return sortedNames.map(name => itemMap[name]);
					}

					obj.log.nodes = topologicalSortKahn(obj.log.nodes, "name", "depends_on")

					return obj;
				}
				function _orchestrateExecute(obj) {
					for (node of obj.log.nodes) {
						
						let timeZone = Session.getScriptTimeZone(); 
						let start = new Date();
						let startFormatted = Utilities.formatDate(start, timeZone, "yyyy-MM-dd HH:mm:ss")
						node.start = startFormatted;

						if(node.type == 'move') {
						if(Array.isArray(node.info)) {
							for(item of node.info) {
							tinyDeskData.pipe.move(item)
							}
						} else {tinyDeskData.pipe.move(node.info)}
						}

						if(node.type == 'transform') {
						if(Array.isArray(node.info)) {
							for(item of node.info) {
							tinyDeskData.pipe.transform(item)
							}
						} else {tinyDeskData.pipe.transform(node.info)}
						}

						let end = new Date();
						let endFormatted = Utilities.formatDate(end, timeZone, "yyyy-MM-dd HH:mm:ss")
						node.end = endFormatted;
					}

					return obj
				}
				function _orchestrateEndLog(obj) {
					let now = new Date();
					let timeZone = Session.getScriptTimeZone(); 
					let formattedDate = Utilities.formatDate(now, timeZone, "yyyy-MM-dd HH:mm:ss")
					obj.log.end = formattedDate;
					


					
					let jsonString = JSON.stringify(obj.log, null, 2);
					let filename = "log_" + obj.name + ".json";
					let mimeType = MimeType.PLAIN_TEXT;

					let folderId = obj.log_destiny.folder_id; 
					let folder = DriveApp.getFolderById(folderId);
					folder.createFile(filename, jsonString, mimeType);
					
					return obj
				}

				

				return {
					move: function(obj) {return _moveLoadData(obj,_moveGetData(obj))}
					,transform : function(obj) {return _pipeline(obj
						,_transformGetRawCode
						,_transformSetDependencies
						,_transformSort
						,_transformCompile
						,_transformExecute
					)}
					,orchestrate : function(obj) {return _pipeline(obj
						,_orchestrateCreateLog
						,_orchestrateSort
						,_orchestrateExecute
						,_orchestrateEndLog
						)}
				}
			}
		)()
	};
  
    this.tinyDeskData = tinyDeskData;
	
	return tinyDeskData;
}).call(this);