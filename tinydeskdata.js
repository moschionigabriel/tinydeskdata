(function() {
	var tinyDeskData = (function() {

		// --- MÉTODOS PRIVADOS (Invisíveis no console.log) ---

		function _moveGetData(obj) {
			let data;
			if (obj.source.where == 'drive') {
				let file_id = obj.source.config.file_id
				let file, sheet_name, sheet
				if (Drive.Files.get(file_id).mimeType == 'application/vnd.google-apps.spreadsheet') {
					file = SpreadsheetApp.openById(file_id)
					if (obj.source.config.sheet_name) { sheet_name = obj.source.config.sheet_name } else { sheet_name = file.getSheets()[0].getName() }
					sheet = file.getSheetByName(sheet_name)
					data = sheet.getDataRange().getDisplayValues()
					return data
				}
				else if (Drive.Files.get(file_id).mimeType == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') {
					file = DriveApp.getFileById(file_id)
					let file_blob = file.getBlob()
					let config = { title: "temp_tdp", mimeType: MimeType.GOOGLE_SHEETS }
					let temp_file = Drive.Files.create(config, file_blob)
					let temp_file_id = temp_file.id
					let file_temp = SpreadsheetApp.openById(temp_file_id)
					if (obj.source.config.sheet_name) { sheet_name = obj.source.config.sheet_name } else { sheet_name = file_temp.getSheets()[0].getName() }
					sheet = file_temp.getSheetByName(sheet_name)
					data = sheet.getDataRange().getDisplayValues()
					Drive.Files.remove(temp_file_id)
					return data
				}
				else if (Drive.Files.get(file_id).mimeType == 'text/csv') {
					file = DriveApp.getFileById(file_id)
					data = Utilities.parseCsv(file.getBlob().getDataAsString("utf-8"))
					return data
				}
			} else if (obj.source.where == 'here') {
				let parent_folder = (obj.source.config.parent_folder) ? obj.source.config.parent_folder + '/' : ''
				let file_name = parent_folder + obj.source.config.file_name
				let file_extension = file_name.match(/\.(.*)/)[1]
				let platform = obj.source.config.platform
				if (file_extension == 'sql' && platform == 'bigquery') {
					let bq_project_id = obj.source.config.credentials.project_id
					let query_string = HtmlService.createHtmlOutputFromFile(file_name).getContent().toString().replace(/\n/g, '')
					let request = { query: query_string, useLegacySql: false }
					let query_result = BigQuery.Jobs.query(request, bq_project_id)
					let job_id = query_result.jobReference.jobId
					let sleepTimeMs = 500
					while (!query_result.jobComplete) { Utilities.sleep(sleepTimeMs); query_result = BigQuery.Jobs.getQueryResults(bq_project_id, job_id) }
					let data = query_result.rows.map(row => row.f.map(cell => cell.v))
					let fields = query_result.schema.fields;
					let header = fields.map(field => field.name)
					data.unshift(header)
					return data
				} else if (file_extension == 'gs') {
					let code = HtmlService.createHtmlOutputFromFile(file_name).getContent();
					data = eval('(' + code.trim() + ')');
					return data
				}
			} else if (obj.source.where == 'sql_platform') {
				let bq_project_id = obj.source.config.credentials.project_id
				let query_string = 'select * from ' + obj.source.config.schema_name + '.' + obj.source.config.table_name;
				let request = { query: query_string, useLegacySql: false }
				let query_result = BigQuery.Jobs.query(request, bq_project_id)
				let job_id = query_result.jobReference.jobId
				let sleepTimeMs = 500
				while (!query_result.jobComplete) { Utilities.sleep(sleepTimeMs); query_result = BigQuery.Jobs.getQueryResults(bq_project_id, job_id) }
				let data = query_result.rows.map(row => row.f.map(cell => cell.v))
				let fields = query_result.schema.fields;
				let header = fields.map(field => field.name)
				data.unshift(header)
				return data
			}
			return data
		}

		function _moveLoadData(obj, data) {
			let num_rows = data.length
			let num_columns = data[0].length
			if (obj.destination.where == 'drive') {
				if (obj.destination.config.file_type == 'sheets') {
					if (obj.destination.config.new_file_flag == false) {
						let existing_file_id, existing_file, existing_sheet_name, existing_sheet;
						if (obj.destination.config.file_id) { existing_file_id = obj.destination.config.file_id } else { existing_file_id = DriveApp.getFilesByName(obj.destination.config.file_name).next().getId() }
						existing_file = SpreadsheetApp.openById(existing_file_id)
						if (obj.destination.config.sheet_name) { existing_sheet_name = obj.destination.config.sheet_name } else { existing_sheet_name = existing_file.getSheets()[0].getName() }
						existing_sheet = existing_file.getSheetByName(existing_sheet_name)
						let existing_target_range
						if (obj.destination.config.write_disposition == 'append') {
							existing_target_range = existing_sheet.getRange(existing_sheet.getLastRow() + 1, 1, num_rows, num_columns);
							existing_target_range.setValues(data);
							SpreadsheetApp.flush()
						} else if (obj.destination.config.write_disposition == 'truncate') {
							let existing_data_range = existing_sheet.getDataRange();
							existing_data_range.clearContent();
							existing_target_range = existing_sheet.getRange(1, 1, num_rows, num_columns);
							existing_target_range.setValues(data);
							SpreadsheetApp.flush()
						}
					} else {
						let file_id, file, file_ss, sheet_name, sheet;
						let config = {
							name: obj.destination.config.file_name,
							mimeType: MimeType.GOOGLE_SHEETS
						}
						file = Drive.Files.create(config)
						file_id = file.id
						file_ss = SpreadsheetApp.openById(file_id)
						if (obj.destination.config.sheet_name) { 
              sheet = file_ss.getSheets()[0];
              sheet.setName(obj.destination.config.sheet_name);
            } else { sheet = file_ss.getSheets()[0] }
						sheet.getRange(1, 1, num_rows, num_columns).setValues(data)
						SpreadsheetApp.flush()
						if (obj.destination.config.folder_id) { let resource; Drive.Files.update(resource, file_id, null, { addParents: obj.destination.config.folder_id, removeParents: file.parents }); }
					}
				} else if (obj.destination.config.file_type == 'xlsx') {
					if (obj.destination.config.new_file_flag == false) {
						let source_file_id, source_file, source_file_blob, source_file_name, temp_file, temp_file_id, file_temp, temp_file_blob, temp_sheet, sheet_name, target_file, target_file_id, folder_id, folder;
						if (obj.destination.config.file_id) { source_file_id = obj.destination.config.file_id } else { source_file_id = DriveApp.getFilesByName(obj.destination.config.file_name).next().getId() }
						source_file = DriveApp.getFileById(source_file_id)
						source_file_blob = source_file.getBlob()
						source_file_name = source_file.getName()
						let config = { name: source_file_name, mimeType: MimeType.GOOGLE_SHEETS }
						temp_file = Drive.Files.create(config, source_file_blob)
						temp_file_id = temp_file.id
						file_temp = SpreadsheetApp.openById(temp_file_id)
						if (obj.source.config.sheet_name) { sheet_name = obj.source.config.sheet_name } else { sheet_name = file_temp.getSheets()[0].getName() }
						temp_sheet = file_temp.getSheetByName(sheet_name)
						if (obj.destination.config.write_disposition == 'append') {
							temp_sheet.getRange(temp_sheet.getLastRow() + 1, 1, num_rows, num_columns).setValues(data);
						} else if (obj.destination.config.write_disposition == 'truncate') {
							temp_sheet.getDataRange().clearContent();
							temp_sheet.getRange(1, 1, num_rows, num_columns).setValues(data);
						}
						SpreadsheetApp.flush()
						var url = 'https://docs.google.com/spreadsheets/d/' + temp_file_id + '/export?format=xlsx';
						var token = ScriptApp.getOAuthToken();
						var response = UrlFetchApp.fetch(url, { headers: { 'Authorization': 'Bearer ' + token } });
						temp_file_blob = response.getBlob().setName(source_file_name);
						folder_id = obj.destination.config.folder_id || source_file.getParents().next().getId()
						folder = DriveApp.getFolderById(folder_id);
						target_file = folder.createFile(temp_file_blob);
						Drive.Files.remove(source_file_id)
						Drive.Files.remove(temp_file_id)
					} else {
						let temp_file_id, temp_file, temp_file_ss, temp_sheet, target_file_name, folder_id, folder, temp_file_blob;
						let config = { name: obj.destination.config.file_name, mimeType: MimeType.GOOGLE_SHEETS }
						temp_file = Drive.Files.create(config)
						temp_file_id = temp_file.id
						temp_file_ss = SpreadsheetApp.openById(temp_file_id)
						temp_sheet = temp_file_ss.getSheets()[0]
						if (obj.destination.config.sheet_name) { temp_sheet.setName(obj.destination.config.sheet_name) }
						temp_sheet.getRange(1, 1, num_rows, num_columns).setValues(data)
						SpreadsheetApp.flush()
						var url = 'https://docs.google.com/spreadsheets/d/' + temp_file_id + '/export?format=xlsx';
						var token = ScriptApp.getOAuthToken();
						var response = UrlFetchApp.fetch(url, { headers: { 'Authorization': 'Bearer ' + token } });
						target_file_name = obj.destination.config.file_name.endsWith('.xlsx') ? obj.destination.config.file_name : obj.destination.config.file_name + '.xlsx';
						temp_file_blob = response.getBlob().setName(target_file_name);
						folder_id = obj.destination.config.folder_id || DriveApp.getRootFolder().getId()
						folder = DriveApp.getFolderById(folder_id);
						folder.createFile(temp_file_blob);
						Drive.Files.remove(temp_file_id)
					}
				} else if (obj.destination.config.file_type == 'csv') {
					let new_data_csv = data.map(row => row.map(cell => {
						const cellStr = String(cell);
						return (cellStr.includes(',') || cellStr.includes('"') || cellStr.includes('\n')) ? '"' + cellStr.replace(/"/g, '""') + '"' : cellStr;
					}).join(',')).join('\n');

					if (obj.destination.config.new_file_flag == false) {
						let source_file_id = obj.destination.config.file_id || DriveApp.getFilesByName(obj.destination.config.file_name).next().getId()
						let source_file = DriveApp.getFileById(source_file_id)
						let source_data = source_file.getBlob().getDataAsString()
						if (source_data.charCodeAt(0) === 0xFEFF) source_data = source_data.substring(1)
						let target_data = (obj.destination.config.write_disposition == 'append') ? source_data.trimEnd() + '\n' + new_data_csv : new_data_csv;
						source_file.setContent('\ufeff' + target_data);
					} else {
						let target_file_name = obj.destination.config.file_name.endsWith('.csv') ? obj.destination.config.file_name : obj.destination.config.file_name + '.csv';
						let folder_id = obj.destination.config.folder_id || DriveApp.getRootFolder().getId()
						DriveApp.getFolderById(folder_id).createFile(target_file_name, '\ufeff' + new_data_csv, MimeType.CSV);
					}
				}
			} else if (obj.destination.where == 'sql_platform' && obj.destination.config.platform == 'bigquery') {
				let bq_project_id = obj.destination.config.credentials.project_id
				let write_disposition = 'WRITE_' + (obj.destination.config.write_disposition || 'append').toUpperCase()
				let headers = data[0];
				let table_schema = {
					fields: headers.map(header => {
						let cleanName = String(header).replace(/[^a-zA-Z0-9_]/g, '_');
						return { name: cleanName, type: (cleanName === obj.destination.config.partition_column) ? 'DATE' : 'STRING' };
					})
				};
				let jsonRows = data.slice(1).map(row => {
					let rowObject = {};
					headers.forEach((header, index) => {
						let cleanHeader = String(header).replace(/[^a-zA-Z0-9_]/g, '_');
						rowObject[cleanHeader] = row[index] !== null && row[index] !== undefined ? String(row[index]) : null;
					});
					return JSON.stringify(rowObject);
				}).join('\n');
				let job = {
					configuration: {
						load: {
							destinationTable: { projectId: bq_project_id, datasetId: obj.destination.config.schema_name, tableId: obj.destination.config.table_name },
							schema: table_schema, sourceFormat: 'NEWLINE_DELIMITED_JSON', writeDisposition: write_disposition
						}
					}
				};
				if (obj.destination.config.partition_column) job.configuration.load.timePartitioning = { type: 'DAY', field: obj.destination.config.partition_column };
				let blob = Utilities.newBlob(jsonRows, 'application/octet-stream');
				const insertJob = BigQuery.Jobs.insert(job, bq_project_id, blob);
				while (BigQuery.Jobs.get(bq_project_id, insertJob.jobReference.jobId).status.state !== 'DONE') { Utilities.sleep(1000) }
			}
		}

		function _pipeline(obj, ...functions) { return functions.reduce((result, fn) => fn(result), obj); }

		function _modelGetRawCode(obj) {
			let parent_folder = (obj.parent_folder) ? obj.parent_folder + '/' : ''
			for (let model of obj.models) {
				let file_name = parent_folder + model.name + '.sql.html'
				model.raw_code = HtmlService.createHtmlOutputFromFile(file_name).getContent().toString().replace(/\n/g, '')
			}
			return obj
		}

		function _modelSetDependencies(obj) {
			for (let model of obj.models) {
				let regex = /{{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*}}/g;
				let dependencies = [];
				let match;
				while ((match = regex.exec(model.raw_code)) !== null) { dependencies.push(match[1]); }
				model.depends_on = dependencies
			}
			return obj
		}

		function _topologicalSort(items, nameKey, dependsOnKey) {
			const graph = {}, inDegree = {}, itemMap = {};
			for (const item of items) {
				const name = item[nameKey];
				graph[name] = []; inDegree[name] = 0; itemMap[name] = item;
			}
			for (const item of items) {
				const dependentName = item[nameKey];
				const dependencies = item[dependsOnKey] || [];
				for (const dependencyName of dependencies) {
					if (graph[dependencyName] && inDegree.hasOwnProperty(dependentName)) {
						graph[dependencyName].push(dependentName);
						inDegree[dependentName]++;
					}
				}
			}
			const queue = [];
			for (const name in inDegree) { if (inDegree[name] === 0) queue.push(name); }
			const sortedNames = [];
			while (queue.length > 0) {
				const uName = queue.shift();
				sortedNames.push(uName);
				for (const vName of graph[uName]) { inDegree[vName]--; if (inDegree[vName] === 0) queue.push(vName); }
			}
			return sortedNames.map(name => itemMap[name]);
		}

		function _modelCompile(obj) {
			for (let model of obj.models) {
				if (model.raw_code) {
					let processedCode = model.raw_code;
					const setRegex = /{%\s*set\s+(\w+)\s*=\s*\[([^\]]+)\]\s*%}/g;
					const variables = {}; let setMatch;
					while ((setMatch = setRegex.exec(processedCode)) !== null) {
						variables[setMatch[1]] = setMatch[2].split(',').map(item => item.trim().replace(/['"]/g, ''));
					}
					processedCode = processedCode.replace(setRegex, '');
					const forRegex = /{%\s*for\s+(\w+)\s+in\s+(\w+)\s*-%}([\s\S]*?){%\s*endfor\s*-%}/g;
					let forMatch;
					while ((forMatch = forRegex.exec(processedCode)) !== null) {
						if (variables[forMatch[2]]) {
							let expanded = variables[forMatch[2]].map(item => forMatch[3].replace(new RegExp(`{{\\s*${forMatch[1]}\\s*}}`, 'g'), item)).join('\n');
							processedCode = processedCode.replace(forMatch[0], expanded);
						}
					}
					const nodeMap = {};
					for (const m of obj.models) nodeMap[m.name] = `${obj.config.credentials.project_id}.${m.schema_name}.${m.name}`;
					model.compiled_code = processedCode.replace(/\{\{\s*ref\((['"])(.*?)\1\)\s*\}\}/g, (match, quote, refName) => nodeMap[refName] || match);
				}
			}
			return obj;
		}

		function _modelExecute(obj) {
			for (let model of obj.models) {
				let run_query, fullTable = `${obj.config.credentials.project_id}.${model.schema_name}.${model.name}`;
				if (model.write_disposition == 'append') {
					run_query = `INSERT INTO ${fullTable} (${model.compiled_code}${model.partition_column ? ' PARTITION BY ' + model.partition_column : ''})`;
				} else {
					run_query = `CREATE OR REPLACE ${model.materialized.toUpperCase()} ${fullTable} AS (${model.compiled_code}${model.partition_column ? ' PARTITION BY ' + model.partition_column : ''})`;
				}
				let queryResults = BigQuery.Jobs.query({ query: run_query, useLegacySql: false }, obj.config.credentials.project_id);
				while (BigQuery.Jobs.get(obj.config.credentials.project_id, queryResults.jobReference.jobId).status.state !== 'DONE') { Utilities.sleep(2000); }
			}
			return obj
		}

		function _orchestrateCreateLog(obj) {
			obj.log = { name: obj.name, start: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"), nodes: JSON.parse(JSON.stringify(obj.nodes)) };
			for (let node of obj.log.nodes) {
				let check = Array.isArray(node.info) ? node.info[0] : node.info;
				node.type = check.source ? 'move' : 'model';
			}
			return obj
		}

		function _orchestrateExecute(obj, pubMove, pubModel) {
			for (let node of obj.log.nodes) {
				node.start = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
				let runner = (node.type == 'move') ? pubMove : pubModel;
				if (Array.isArray(node.info)) { for (let item of node.info) runner(item) } else { runner(node.info) }
				node.end = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
			}
			return obj
		}

		function _orchestrateEndLog(obj) {
			obj.log.end = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
			DriveApp.getFolderById(obj.log_destination.folder_id).createFile("log_" + obj.name + ".json", JSON.stringify(obj.log, null, 2), MimeType.PLAIN_TEXT);
			return obj
		}

		// --- API PÚBLICA ---

		const api = {
			move: function(obj) { return _moveLoadData(obj, _moveGetData(obj)); },
			model: function(obj) {
				return _pipeline(obj, _modelGetRawCode, _modelSetDependencies, (o) => { o.models = _topologicalSort(o.models, "name", "depends_on"); return o; }, _modelCompile, _modelExecute);
			},
			orchestrate: function(obj) {
				return _pipeline(obj,
					_orchestrateCreateLog,
					(o) => { o.log.nodes = _topologicalSort(o.log.nodes, "name", "depends_on"); return o; },
					(o) => _orchestrateExecute(o, api.move, api.model),
					_orchestrateEndLog
				);
			}
		};

		return api;

	})();

	this.tinyDeskData = tinyDeskData;
	return tinyDeskData;
}).call(this);