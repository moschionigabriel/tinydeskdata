(function() {
	var tinyDeskData = (function() {

		// --- MÉTODOS PRIVADOS (Invisíveis no console.log) ---

		function _moveGetData(obj) {
			let data;
			if (obj.source.where == 'drive') {
				let file_id = obj.source.config.file_id;
				let file, sheet_name, sheet;
				let mimeType = Drive.Files.get(file_id).mimeType;
				
				if (mimeType == 'application/vnd.google-apps.spreadsheet') {
					file = SpreadsheetApp.openById(file_id);
					sheet_name = obj.source.config.sheet_name || file.getSheets()[0].getName();
					sheet = file.getSheetByName(sheet_name);
					return sheet.getDataRange().getDisplayValues();
				}
				else if (mimeType == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') {
					file = DriveApp.getFileById(file_id);
					let temp_file = Drive.Files.create({ title: "temp_tdp", mimeType: MimeType.GOOGLE_SHEETS }, file.getBlob());
					let file_temp = SpreadsheetApp.openById(temp_file.id);
					sheet_name = obj.source.config.sheet_name || file_temp.getSheets()[0].getName();
					data = file_temp.getSheetByName(sheet_name).getDataRange().getDisplayValues();
					Drive.Files.remove(temp_file.id);
					return data;
				}
				else if (mimeType == 'text/csv') {
					return Utilities.parseCsv(DriveApp.getFileById(file_id).getBlob().getDataAsString("utf-8"));
				}
			} else if (obj.source.where == 'here') {
				let parent_folder = (obj.source.config.parent_folder) ? obj.source.config.parent_folder + '/' : '';
				let file_name = parent_folder + obj.source.config.file_name;
				let file_extension = file_name.match(/\.(.*)/)[1];
				if (file_extension == 'sql' && obj.source.config.platform == 'bigquery') {
					let bq_project_id = obj.source.config.credentials.project_id;
					let query_string = HtmlService.createHtmlOutputFromFile(file_name).getContent().toString().replace(/\n/g, '');
					let query_result = BigQuery.Jobs.query({ query: query_string, useLegacySql: false }, bq_project_id);
					while (!query_result.jobComplete) { Utilities.sleep(500); query_result = BigQuery.Jobs.getQueryResults(bq_project_id, query_result.jobReference.jobId); }
					let res_data = query_result.rows.map(row => row.f.map(cell => cell.v));
					res_data.unshift(query_result.schema.fields.map(field => field.name));
					return res_data;
				} else if (file_extension == 'gs') {
					return eval('(' + HtmlService.createHtmlOutputFromFile(file_name).getContent().trim() + ')');
				}
			} else if (obj.source.where == 'sql_platform') {
				let bq_project_id = obj.source.config.credentials.project_id;
				let query_string = 'select * from ' + obj.source.config.schema_name + '.' + obj.source.config.table_name;
				let query_result = BigQuery.Jobs.query({ query: query_string, useLegacySql: false }, bq_project_id);
				while (!query_result.jobComplete) { Utilities.sleep(500); query_result = BigQuery.Jobs.getQueryResults(bq_project_id, query_result.jobReference.jobId); }
				let res_data = query_result.rows.map(row => row.f.map(cell => cell.v));
				res_data.unshift(query_result.schema.fields.map(field => field.name));
				return res_data;
			}
			return data;
		}

		function _moveLoadData(obj, data) {
			let num_rows = data.length;
			let num_columns = data[0].length;
			if (obj.destination.where == 'drive') {
				if (obj.destination.config.file_type == 'sheets') {
					let ss, sheet;
					if (obj.destination.config.new_file_flag == false) {
						let id = obj.destination.config.file_id || DriveApp.getFilesByName(obj.destination.config.file_name).next().getId();
						ss = SpreadsheetApp.openById(id);
						sheet = obj.destination.config.sheet_name ? ss.getSheetByName(obj.destination.config.sheet_name) : ss.getSheets()[0];
						if (obj.destination.config.write_disposition == 'append') {
							sheet.getRange(sheet.getLastRow() + 1, 1, num_rows, num_columns).setValues(data);
						} else {
							sheet.getDataRange().clearContent();
							sheet.getRange(1, 1, num_rows, num_columns).setValues(data);
						}
					} else {
						let file = Drive.Files.create({ name: obj.destination.config.file_name, mimeType: MimeType.GOOGLE_SHEETS });
						ss = SpreadsheetApp.openById(file.id);
						sheet = ss.getSheets()[0];
						if (obj.destination.config.sheet_name) sheet.setName(obj.destination.config.sheet_name);
						sheet.getRange(1, 1, num_rows, num_columns).setValues(data);
						if (obj.destination.config.folder_id) Drive.Files.update({}, file.id, null, { addParents: obj.destination.config.folder_id, removeParents: file.parents });
					}
					SpreadsheetApp.flush();
				} else if (obj.destination.config.file_type == 'xlsx' || obj.destination.config.file_type == 'csv') {
                    // Lógica de CSV e XLSX simplificada para garantir funcionamento
                    if (obj.destination.config.file_type == 'csv') {
                        let csvContent = data.map(row => row.map(c => {
                            let s = String(c);
                            return (s.includes(',') || s.includes('"') || s.includes('\n')) ? '"' + s.replace(/"/g, '""') + '"' : s;
                        }).join(',')).join('\n');
                        let fileName = obj.destination.config.file_name.endsWith('.csv') ? obj.destination.config.file_name : obj.destination.config.file_name + '.csv';
                        let folder = DriveApp.getFolderById(obj.destination.config.folder_id || DriveApp.getRootFolder().getId());
                        folder.createFile(fileName, '\ufeff' + csvContent, MimeType.CSV);
                    }
                    // Adicione aqui a sua lógica específica de XLSX se necessário
                }
			} else if (obj.destination.where == 'sql_platform' && obj.destination.config.platform == 'bigquery') {
				let bq_id = obj.destination.config.credentials.project_id;
				let headers = data[0];
				let schema = { fields: headers.map(h => {
					let n = String(h).replace(/[^a-zA-Z0-9_]/g, '_');
					return { name: n, type: (n === obj.destination.config.partition_column ? 'DATE' : 'STRING') };
				})};
				let rows = data.slice(1).map(r => {
					let o = {};
					headers.forEach((h, i) => o[String(h).replace(/[^a-zA-Z0-9_]/g, '_')] = r[i] != null ? String(r[i]) : null);
					return JSON.stringify(o);
				}).join('\n');
				let job = { configuration: { load: {
					destinationTable: { projectId: bq_id, datasetId: obj.destination.config.schema_name, tableId: obj.destination.config.table_name },
					schema: schema, sourceFormat: 'NEWLINE_DELIMITED_JSON', writeDisposition: 'WRITE_' + (obj.destination.config.write_disposition || 'append').toUpperCase()
				}}};
				if (obj.destination.config.partition_column) job.configuration.load.timePartitioning = { type: 'DAY', field: obj.destination.config.partition_column };
				let res = BigQuery.Jobs.insert(job, bq_id, Utilities.newBlob(rows, 'application/octet-stream'));
				while (BigQuery.Jobs.get(bq_id, res.jobReference.jobId).status.state !== 'DONE') Utilities.sleep(1000);
			}
		}

		function _pipeline(obj, ...functions) { return functions.reduce((result, fn) => fn(result), obj); }

		function _topologicalSort(items, nameKey, dependsOnKey) {
			const graph = {}, inDegree = {}, itemMap = {};
			items.forEach(item => {
				graph[item[nameKey]] = []; inDegree[item[nameKey]] = 0; itemMap[item[nameKey]] = item;
			});
			items.forEach(item => {
				(item[dependsOnKey] || []).forEach(dep => {
					if (graph[dep]) { graph[dep].push(item[nameKey]); inDegree[item[nameKey]]++; }
				});
			});
			const queue = Object.keys(inDegree).filter(k => inDegree[k] === 0);
			const sorted = [];
			while (queue.length) {
				const u = queue.shift(); sorted.push(u);
				graph[u].forEach(v => { inDegree[v]--; if (inDegree[v] === 0) queue.push(v); });
			}
			return sorted.map(n => itemMap[n]);
		}

		function _modelGetRawCode(obj) {
			let folder = obj.parent_folder ? obj.parent_folder + '/' : '';
			obj.models.forEach(m => m.raw_code = HtmlService.createHtmlOutputFromFile(folder + m.name + '.sql.html').getContent().toString().replace(/\n/g, ''));
			return obj;
		}

		function _modelSetDependencies(obj) {
			obj.models.forEach(m => {
				let match, deps = [], regex = /{{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*}}/g;
				while ((match = regex.exec(m.raw_code)) !== null) deps.push(match[1]);
				m.depends_on = deps;
			});
			return obj;
		}

		function _modelCompile(obj) {
			obj.models.forEach(m => {
				if (!m.raw_code) return;
				let code = m.raw_code;
				// Jinja Simple logic
				const setRegex = /{%\s*set\s+(\w+)\s*=\s*\[([^\]]+)\]\s*%}/g;
				const vars = {}; let mSet;
				while ((mSet = setRegex.exec(code)) !== null) vars[mSet[1]] = mSet[2].split(',').map(i => i.trim().replace(/['"]/g, ''));
				code = code.replace(setRegex, '');
				const forRegex = /{%\s*for\s+(\w+)\s+in\s+(\w+)\s*-%}([\s\S]*?){%\s*endfor\s*-%}/g;
				let mFor;
				while ((mFor = forRegex.exec(code)) !== null) {
					if (vars[mFor[2]]) {
						let exp = vars[mFor[2]].map(item => mFor[3].replace(new RegExp(`{{\\s*${mFor[1]}\\s*}}`, 'g'), item)).join('\n');
						code = code.replace(mFor[0], exp);
					}
				}
				const map = {}; obj.models.forEach(n => map[n.name] = `${obj.config.credentials.project_id}.${n.schema_name}.${n.name}`);
				m.compiled_code = code.replace(/\{\{\s*ref\((['"])(.*?)\1\)\s*\}\}/g, (match, q, name) => map[name] || match);
			});
			return obj;
		}

		function _modelExecute(obj) {
			obj.models.forEach(m => {
				let projectId = obj.config.credentials.project_id;
				let materialized = m.materialized ? m.materialized.toLowerCase() : 'table';
				
				// --- CENÁRIO A: VIEW ---
				if (materialized === 'view') {
				let tableResource = {
					tableReference: {
					projectId: projectId,
					datasetId: m.schema_name,
					tableId: m.name
					},
					view: {
					query: m.compiled_code,
					useLegacySql: false
					}
				};

				// Tenta remover a view/tabela existente primeiro (Simula o CREATE OR REPLACE)
				try {
					BigQuery.Tables.remove(projectId, m.schema_name, m.name);
				} catch (e) {
					// Se der erro aqui, é provável que a tabela não existisse, o que é ok.
				}

				// Agora cria a nova View
				BigQuery.Tables.insert(tableResource, projectId, m.schema_name);

				// --- CENÁRIO B: TABLE OU INSERT ---
				} else {
				let isInsertMode = (materialized === 'insert');
				let disposition = (isInsertMode || m.write_disposition === 'append') ? 'WRITE_APPEND' : 'WRITE_TRUNCATE';

				let jobResource = {
					configuration: {
					query: {
						query: m.compiled_code,
						useLegacySql: false,
						destinationTable: {
						projectId: projectId,
						datasetId: m.schema_name,
						tableId: m.name
						},
						writeDisposition: disposition,
						createDisposition: 'CREATE_IF_NEEDED'
					}
					}
				};

				if (m.partition_column) {
					jobResource.configuration.query.timePartitioning = {
					type: 'DAY',
					field: m.partition_column
					};
				}

				let job = BigQuery.Jobs.insert(jobResource, projectId);
				while (BigQuery.Jobs.get(projectId, job.jobReference.jobId).status.state !== 'DONE') {
					Utilities.sleep(2000);
				}
				}
			});

			return obj;
		}

		function _orchestrateCreateLog(obj) {
			obj.log = { 
				name: obj.name, 
				start: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"), 
				nodes: JSON.parse(JSON.stringify(obj.nodes)) 
			};
			obj.log.nodes.forEach(n => {
				let check = Array.isArray(n.info) ? n.info[0] : n.info;
				n.type = check.source ? 'move' : 'model';
			});
			return obj;
		}

		function _orchestrateExecute(obj, pubApi) {
			obj.log.nodes.forEach(node => {
				node.start = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
				let runner = (node.type == 'move') ? pubApi.move : pubApi.model;
				if (Array.isArray(node.info)) node.info.forEach(item => runner(item));
				else runner(node.info);
				node.end = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
			});
			return obj;
		}

		function _orchestrateEndLog(obj) {
			obj.log.end = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
			let folderId = obj.log_destination.folder_id;
            if (folderId) {
                let folder = DriveApp.getFolderById(folderId);
                folder.createFile("log_" + obj.name + ".json", JSON.stringify(obj.log, null, 2), MimeType.PLAIN_TEXT);
            }
			return obj;
		}

		// --- API PÚBLICA (Único que aparece no console.log) ---

		const api = {
			move: function(obj) { return _moveLoadData(obj, _moveGetData(obj)); },
			model: function(obj) {
				return _pipeline(obj,
					_modelGetRawCode,
					_modelSetDependencies,
					(o) => { o.models = _topologicalSort(o.models, "name", "depends_on"); return o; },
					_modelCompile,
					_modelExecute
				);
			},
			orchestrate: function(obj) {
				return _pipeline(obj,
					_orchestrateCreateLog,
					(o) => { o.log.nodes = _topologicalSort(o.log.nodes, "name", "depends_on"); return o; },
					(o) => _orchestrateExecute(o, api), // Passa a própria API para o executor
					_orchestrateEndLog
				);
			}
		};

		return api;

	})();

	this.tinyDeskData = tinyDeskData;
	return tinyDeskData;
}).call(this);