(function() {
	var tinyDeskData = (function() {

		// --- MÉTODOS PRIVADOS ---

		// --- move: shared helpers ---

		function _bqRunQuery(query_string, project_id) {
			let query_result = BigQuery.Jobs.query({ query: query_string, useLegacySql: false }, project_id);
			while (!query_result.jobComplete) {
				Utilities.sleep(500);
				query_result = BigQuery.Jobs.getQueryResults(project_id, query_result.jobReference.jobId);
			}
			let res_data = query_result.rows.map(row => row.f.map(cell => cell.v));
			res_data.unshift(query_result.schema.fields.map(field => field.name));
			return res_data;
		}

		function _bqPollLoadJob(project_id, job_id) {
			let status;
			while ((status = BigQuery.Jobs.get(project_id, job_id).status).state !== 'DONE') Utilities.sleep(1000);
			return status;
		}

		function _bqSanitizeColumnName(name) {
			return String(name).replace(/[^a-zA-Z0-9_]/g, '_');
		}

		function _readSheetDisplayValues(spreadsheet, sheet_name) {
			let sheet = sheet_name ? spreadsheet.getSheetByName(sheet_name) : spreadsheet.getSheets()[0];
			return sheet.getDataRange().getDisplayValues();
		}

		function _buildCsvContent(data) {
			return data.map(row => row.map(c => {
				let s = String(c);
				return (s.includes(',') || s.includes('"') || s.includes('\n')) ? '"' + s.replace(/"/g, '""') + '"' : s;
			}).join(',')).join('\n');
		}

		// --- move: source ---

		function _moveGetDataFromGoogleSheet(file_id, config) {
			return _readSheetDisplayValues(SpreadsheetApp.openById(file_id), config.sheet_name);
		}

		function _moveGetDataFromExcel(file_id, config) {
			let file = DriveApp.getFileById(file_id);
			let temp_file = Drive.Files.create({ title: "temp_tdp", mimeType: MimeType.GOOGLE_SHEETS }, file.getBlob());
			let data = _readSheetDisplayValues(SpreadsheetApp.openById(temp_file.id), config.sheet_name);
			try {
				Drive.Files.remove(temp_file.id);
			} catch (e) {
				console.warn(`move(): failed to remove temporary file "${temp_file.id}" created while converting Excel file_id "${file_id}" to a Google Sheet: ${e.message}. The source data was still read successfully, but the temp file may need manual cleanup in Drive.`);
			}
			return data;
		}

		function _moveGetDataFromCsv(file_id) {
			return Utilities.parseCsv(DriveApp.getFileById(file_id).getBlob().getDataAsString("utf-8"));
		}

		function _moveGetDataFromDrive(config) {
			let file_id = config.file_id;
			let mimeType = Drive.Files.get(file_id).mimeType;
			if (mimeType == 'application/vnd.google-apps.spreadsheet') return _moveGetDataFromGoogleSheet(file_id, config);
			if (mimeType == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') return _moveGetDataFromExcel(file_id, config);
			if (mimeType == 'text/csv') return _moveGetDataFromCsv(file_id);
			throw new Error(`move(): unsupported source — drive file_id "${file_id}" has mimeType "${mimeType}", which is not one of the supported types (Google Sheet, Excel .xlsx, or CSV).`);
		}

		function _moveGetDataFromHereSql(file_name, config) {
			let query_string = HtmlService.createHtmlOutputFromFile(file_name).getContent().toString().replace(/\n/g, '');
			return _bqRunQuery(query_string, config.credentials.project_id);
		}

		function _moveGetDataFromHereGs(file_name) {
			return eval('(' + HtmlService.createHtmlOutputFromFile(file_name).getContent().trim() + ')');
		}

		function _moveGetDataFromHere(config) {
			let parent_folder = config.parent_folder ? config.parent_folder + '/' : '';
			let file_name = parent_folder + config.file_name;
			let file_extension = file_name.match(/\.(.*)/)[1];
			if (file_extension == 'sql' && config.platform == 'bigquery') return _moveGetDataFromHereSql(file_name, config);
			if (file_extension == 'gs') return _moveGetDataFromHereGs(file_name);
			throw new Error(`move(): unsupported source — here file "${file_name}" has extension ".${file_extension}", which is not supported (expected ".sql" with source.config.platform === 'bigquery', or ".gs").`);
		}

		function _moveGetDataFromSqlPlatform(config) {
			let query_string = 'select * from ' + config.schema_name + '.' + config.table_name;
			return _bqRunQuery(query_string, config.credentials.project_id);
		}

		function _moveGetData(obj) {
			if (obj.source.where == 'drive') return _moveGetDataFromDrive(obj.source.config);
			if (obj.source.where == 'here') return _moveGetDataFromHere(obj.source.config);
			if (obj.source.where == 'sql_platform') return _moveGetDataFromSqlPlatform(obj.source.config);
			throw new Error(`move(): unsupported source.where "${obj.source.where}" — expected 'drive', 'here', or 'sql_platform'.`);
		}

		// --- move: destination ---

		function _moveLoadDataToSheetsExisting(config, data, num_rows, num_columns) {
			let id = config.file_id || DriveApp.getFilesByName(config.file_name).next().getId();
			let ss = SpreadsheetApp.openById(id);
			let sheet = config.sheet_name ? ss.getSheetByName(config.sheet_name) : ss.getSheets()[0];
			if (config.write_disposition == 'append') {
				sheet.getRange(sheet.getLastRow() + 1, 1, num_rows, num_columns).setValues(data);
			} else {
				sheet.getDataRange().clearContent();
				sheet.getRange(1, 1, num_rows, num_columns).setValues(data);
			}
		}

		function _moveLoadDataToSheetsNew(config, data, num_rows, num_columns) {
			let file = Drive.Files.create({ name: config.file_name, mimeType: MimeType.GOOGLE_SHEETS });
			let ss = SpreadsheetApp.openById(file.id);
			let sheet = ss.getSheets()[0];
			if (config.sheet_name) sheet.setName(config.sheet_name);
			sheet.getRange(1, 1, num_rows, num_columns).setValues(data);
			if (config.folder_id) Drive.Files.update({}, file.id, null, { addParents: config.folder_id, removeParents: file.parents });
		}

		function _moveLoadDataToSheets(config, data, num_rows, num_columns) {
			if (config.new_file_flag == false) {
				_moveLoadDataToSheetsExisting(config, data, num_rows, num_columns);
			} else {
				_moveLoadDataToSheetsNew(config, data, num_rows, num_columns);
			}
			SpreadsheetApp.flush();
		}

		function _moveLoadDataToCsv(config, data) {
			let csvContent = _buildCsvContent(data);
			let fileName = config.file_name.endsWith('.csv') ? config.file_name : config.file_name + '.csv';
			let folder = DriveApp.getFolderById(config.folder_id || DriveApp.getRootFolder().getId());
			folder.createFile(fileName, '\ufeff' + csvContent, MimeType.CSV);
		}

		function _moveLoadDataToDrive(config, data, num_rows, num_columns) {
			if (config.file_type == 'sheets') return _moveLoadDataToSheets(config, data, num_rows, num_columns);
			if (config.file_type == 'csv') return _moveLoadDataToCsv(config, data);
			throw new Error(`move(): unsupported destination — drive file_type "${config.file_type}" is not one of the supported types ('sheets' or 'csv').`);
		}

		function _moveLoadDataToSqlPlatform(config, data) {
			let bq_id = config.credentials.project_id;
			let headers = data[0];
			let schema = { fields: headers.map(h => {
				let n = _bqSanitizeColumnName(h);
				return { name: n, type: (n === config.partition_column ? 'DATE' : 'STRING') };
			})};
			let rows = data.slice(1).map(r => {
				let o = {};
				headers.forEach((h, i) => o[_bqSanitizeColumnName(h)] = r[i] != null ? String(r[i]) : null);
				return JSON.stringify(o);
			}).join('\n');
			let job = { configuration: { load: {
				destinationTable: { projectId: bq_id, datasetId: config.schema_name, tableId: config.table_name },
				schema: schema, sourceFormat: 'NEWLINE_DELIMITED_JSON', writeDisposition: 'WRITE_' + (config.write_disposition || 'append').toUpperCase()
			}}};
			if (config.partition_column) job.configuration.load.timePartitioning = { type: 'DAY', field: config.partition_column };
			let res = BigQuery.Jobs.insert(job, bq_id, Utilities.newBlob(rows, 'application/octet-stream'));
			let jobStatus = _bqPollLoadJob(bq_id, res.jobReference.jobId);
			if (jobStatus.errorResult) {
				throw new Error(`move(): BigQuery load into ${config.schema_name}.${config.table_name} failed: ${jobStatus.errorResult.message}`);
			}
		}

		function _moveLoadData(obj, data) {
			let num_rows = data.length;
			let num_columns = data[0].length;
			if (obj.destination.where == 'drive') {
				_moveLoadDataToDrive(obj.destination.config, data, num_rows, num_columns);
			} else if (obj.destination.where == 'sql_platform' && obj.destination.config.platform == 'bigquery') {
				_moveLoadDataToSqlPlatform(obj.destination.config, data);
			} else {
				throw new Error(`move(): unsupported destination.where "${obj.destination.where}"${obj.destination.config && obj.destination.config.platform ? ` (platform "${obj.destination.config.platform}")` : ''} — expected 'drive', or 'sql_platform' with config.platform === 'bigquery'.`);
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
			obj.models.forEach(m => m.raw_code = HtmlService.createTemplateFromFile(folder + m.name + '.sql.html').getRawContent().replace(/\n/g, ' '));
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
				
				const setRegex = /{%\s*set\s+(\w+)\s*=\s*\[([^\]]+)\]\s*%}/g;
				const vars = {}; let mSet;
				while ((mSet = setRegex.exec(code)) !== null) {
				vars[mSet[1]] = mSet[2].split(',').map(i => i.trim().replace(/['"]/g, ''));
				}
				code = code.replace(setRegex, '');
				
				let projectId = obj.config.credentials.project_id;
				let tableExists = false;
				let materialized = (m.materialized || 'table').toLowerCase();
				
				if (materialized === 'incremental') {
				try {
					BigQuery.Tables.get(projectId, m.schema_name, m.name);
					tableExists = true;
				} catch (e) { tableExists = false; }
				}

				const isIncrementalVal = tableExists;

				const ifIncrementalRegex = /{%\s*if\s+is_incremental\(\s*\)\s*%}([\s\S]*?){%\s*endif\s*%}/g;
				code = code.replace(ifIncrementalRegex, (match, content) => {
				return isIncrementalVal ? content : '';
				});

				const forRegex = /{%\s*for\s+(\w+)\s+in\s+(\w+)\s*-%}([\s\S]*?){%\s*endfor\s*-%}/g;
				let mFor;
				while ((mFor = forRegex.exec(code)) !== null) {
					if (vars[mFor[2]]) {
						let exp = vars[mFor[2]].map(item => mFor[3].replace(new RegExp(`{{\\s*${mFor[1]}\\s*}}`, 'g'), item)).join('\n');
						code = code.replace(mFor[0], exp);
					}
				}
				
				code = code.replace(/{{\s*is_incremental\(\s*\)\s*}}/g, isIncrementalVal ? 'true' : 'false');
				const map = {}; 
				obj.models.forEach(n => map[n.name] = `${projectId}.${n.schema_name}.${n.name}`);
				code = code.replace(/\{\{\s*ref\((['"])(.*?)\1\)\s*\}\}/g, (match, q, name) => map[name] || match);
				
				m.compiled_code = code;
				m._table_exists = tableExists;
				//console.log("DEBUG SQL Gerado para " + m.name + ": " + m.compiled_code);
			});
			return obj;
		}

		function _modelRunTests(obj, m, tempTableName) {
			let testResults = { pass: true, details: [] };
			if (!m.columns) return testResults;
			
			let projectId = obj.config.credentials.project_id;

			m.columns.forEach(col => {
				if (!col.tests) return;
				col.tests.forEach(test => {
					let testName = typeof test === 'string' ? test : Object.keys(test)[0];
					let tableRef = `\`${projectId}.${m.schema_name}.${tempTableName}\``;
					let query = "";

					if (testName === 'unique') query = `SELECT ${col.name}, COUNT(*) as c FROM ${tableRef} GROUP BY 1 HAVING c > 1 LIMIT 1`;
					else if (testName === 'not null') query = `SELECT ${col.name} FROM ${tableRef} WHERE ${col.name} IS NULL LIMIT 1`;
					else if (testName === 'accepted_values') {
						let values = test.accepted_values.values.map(v => `'${v}'`).join(',');
						query = `SELECT ${col.name} FROM ${tableRef} WHERE ${col.name} NOT IN (${values}) LIMIT 1`;
					}
					else if (testName === 'relationships') {
						let toRef = test.relationships[0].to.match(/['"]([^'"]+)['"]/)[1];
						let toField = test.relationships[1].field;
						let targetModel = obj.models.find(mod => mod.name === toRef);
						let targetTable = `\`${projectId}.${targetModel.schema_name}.${targetModel.name}\``;
						query = `SELECT a.${col.name} FROM ${tableRef} a LEFT JOIN ${targetTable} b ON a.${col.name} = b.${toField} WHERE b.${toField} IS NULL AND a.${col.name} IS NOT NULL LIMIT 1`;
					}

					if (query) {
						let testRes = BigQuery.Jobs.query({ query: query, useLegacySql: false }, projectId);
						let failCount = parseInt(testRes.totalRows);
						let status = failCount === 0 ? 'PASS' : 'FAIL';
						
						testResults.details.push({ column: col.name, test: testName, status: status, rows_failed: failCount });
						if (status === 'FAIL') testResults.pass = false;
					}
				});
			});
			return testResults;
		}

		function _modelExecute(obj) {
			let projectId = obj.config.credentials.project_id;
			
			obj.models.forEach(m => {
				let materialized = (m.materialized || 'table').toLowerCase();
				let tempTableName = m.name + "__tmp";

				let jobResource = {
					configuration: { query: {
						query: m.compiled_code, 
						useLegacySql: false,
						destinationTable: { projectId: projectId, datasetId: m.schema_name, tableId: tempTableName },
						writeDisposition: 'WRITE_TRUNCATE', 
						createDisposition: 'CREATE_IF_NEEDED'
					}}
				};

				let job = BigQuery.Jobs.insert(jobResource, projectId);
				let status;

				while (true) {
					status = BigQuery.Jobs.get(projectId, job.jobReference.jobId).status;
					if (status.state === 'DONE') break;
					Utilities.sleep(1000);
				}

				if (status.errorResult) {
					throw new Error(`[ERRO NO MODELO: ${m.name}] ${status.errorResult.message}`);
				}

				let testResults = _modelRunTests(obj, m, tempTableName);
				m.test_results = testResults.details; 

				if (!testResults.pass) {
					BigQuery.Tables.remove(projectId, m.schema_name, tempTableName);
					throw new Error(`[CRITICAL] tests failed in ${m.name}. pipeline aborted.`);
				}

				if (materialized === 'view') {
					let viewRes = { tableReference: { projectId, datasetId: m.schema_name, tableId: m.name }, view: { query: m.compiled_code, useLegacySql: false } };
					try { BigQuery.Tables.remove(projectId, m.schema_name, m.name); } catch(e){}
					BigQuery.Tables.insert(viewRes, projectId, m.schema_name);
				} 
				else if (materialized === 'incremental') {
					_executeIncremental(obj, m, tempTableName, projectId);
				}
				else {

					let isAppend = (materialized === 'insert' || m.write_disposition === 'append');
					let copyJob = { configuration: { query: {
						query: `SELECT * FROM \`${projectId}.${m.schema_name}.${tempTableName}\``,
						destinationTable: { projectId, datasetId: m.schema_name, tableId: m.name },
						writeDisposition: isAppend ? 'WRITE_APPEND' : 'WRITE_TRUNCATE', 
						useLegacySql: false
					}}};
					if (m.partition_column) copyJob.configuration.query.timePartitioning = { type: 'DAY', field: m.partition_column };
					let res = BigQuery.Jobs.insert(copyJob, projectId);
					
					while (BigQuery.Jobs.get(projectId, res.jobReference.jobId).status.state !== 'DONE') Utilities.sleep(1000);
				}

				_applyMetadata(projectId, m);
				BigQuery.Tables.remove(projectId, m.schema_name, tempTableName);
				console.log(`[OK] ${m.name} processed successfully.`);
			});
			return obj;
		}

		function _executeIncremental(obj, m, tempTableName, projectId) {
			let strategy = m.incremental_strategy || 'append';
			let tableExists = m._table_exists;

			if (!tableExists) {
				let createJob = { configuration: { query: {
					query: `SELECT * FROM \`${projectId}.${m.schema_name}.${tempTableName}\``,
					destinationTable: { projectId, datasetId: m.schema_name, tableId: m.name },
					writeDisposition: 'WRITE_TRUNCATE',
					useLegacySql: false
				}}};
				if (m.partition_column) createJob.configuration.query.timePartitioning = { type: 'DAY', field: m.partition_column };
				let res = BigQuery.Jobs.insert(createJob, projectId);
				while (BigQuery.Jobs.get(projectId, res.jobReference.jobId).status.state !== 'DONE') Utilities.sleep(1000);
				console.log(`[INCREMENTAL] ${m.name} - primeira execução (full refresh)`);
				return;
			}

			if (strategy === 'append') {
				let appendJob = { configuration: { query: {
					query: `SELECT * FROM \`${projectId}.${m.schema_name}.${tempTableName}\``,
					destinationTable: { projectId, datasetId: m.schema_name, tableId: m.name },
					writeDisposition: 'WRITE_APPEND',
					useLegacySql: false
				}}};
				let res = BigQuery.Jobs.insert(appendJob, projectId);
				while (BigQuery.Jobs.get(projectId, res.jobReference.jobId).status.state !== 'DONE') Utilities.sleep(1000);
				console.log(`[INCREMENTAL] ${m.name} - append strategy`);
			}
			else if (strategy === 'merge' || strategy === 'delete+insert') {
				if (!m.unique_key) throw new Error(`[ERRO] ${m.name}: estratégia '${strategy}' requer 'unique_key'`);
				
				let uniqueKeys = Array.isArray(m.unique_key) ? m.unique_key : [m.unique_key];
				let targetTable = `\`${projectId}.${m.schema_name}.${m.name}\``;
				let sourceTable = `\`${projectId}.${m.schema_name}.${tempTableName}\``;
				
				if (strategy === 'delete+insert') {
					let deleteConditions = uniqueKeys.map(k => `target.${k} = source.${k}`).join(' AND ');
					let deleteQuery = `
						DELETE FROM ${targetTable} AS target
						WHERE EXISTS (
							SELECT 1 FROM ${sourceTable} AS source
							WHERE ${deleteConditions}
						)
					`;
					let delJob = BigQuery.Jobs.query({ query: deleteQuery, useLegacySql: false }, projectId);
					while (!delJob.jobComplete) { 
						Utilities.sleep(500); 
						delJob = BigQuery.Jobs.getQueryResults(projectId, delJob.jobReference.jobId); 
					}
					
					let insertJob = { configuration: { query: {
						query: `SELECT * FROM ${sourceTable}`,
						destinationTable: { projectId, datasetId: m.schema_name, tableId: m.name },
						writeDisposition: 'WRITE_APPEND',
						useLegacySql: false
					}}};
					let res = BigQuery.Jobs.insert(insertJob, projectId);
					while (BigQuery.Jobs.get(projectId, res.jobReference.jobId).status.state !== 'DONE') Utilities.sleep(1000);
					console.log(`[INCREMENTAL] ${m.name} - delete+insert strategy`);
				}
				else {
					let matchCondition = uniqueKeys.map(k => `target.${k} = source.${k}`).join(' AND ');
					
					let tempTableInfo = BigQuery.Tables.get(projectId, m.schema_name, tempTableName);
					let columns = tempTableInfo.schema.fields.map(f => f.name);
					let updateSet = columns.map(c => `target.${c} = source.${c}`).join(', ');
					let insertCols = columns.join(', ');
					let insertVals = columns.map(c => `source.${c}`).join(', ');
					
					let mergeQuery = `
						MERGE ${targetTable} AS target
						USING ${sourceTable} AS source
						ON ${matchCondition}
						WHEN MATCHED THEN
							UPDATE SET ${updateSet}
						WHEN NOT MATCHED THEN
							INSERT (${insertCols})
							VALUES (${insertVals})
					`;
					
					let mergeJob = BigQuery.Jobs.query({ query: mergeQuery, useLegacySql: false }, projectId);
					while (!mergeJob.jobComplete) { 
						Utilities.sleep(500); 
						mergeJob = BigQuery.Jobs.getQueryResults(projectId, mergeJob.jobReference.jobId); 
					}
					console.log(`[INCREMENTAL] ${m.name} - merge strategy`);
				}
			}
		}

		function _applyMetadata(projectId, m) {
			if (!m.description && !m.columns) return;
			try {
				let table = BigQuery.Tables.get(projectId, m.schema_name, m.name);
				let patchResource = { description: m.description };
				if (m.columns && table.schema && table.schema.fields) {
					patchResource.schema = { fields: table.schema.fields };
					patchResource.schema.fields.forEach(f => {
						let cfg = m.columns.find(c => c.name === f.name);
						if (cfg && cfg.description) f.description = cfg.description;
						delete f.precision; delete f.scale;
					});
				}
				BigQuery.Tables.patch(patchResource, projectId, m.schema_name, m.name);
			} catch (e) {}
		}

		// --- ORQUESTRAÇÃO ---

		function _orchestrateCreateLog(obj) {
			obj.log = { 
				name: obj.name, 
				start: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"), 
				nodes: JSON.parse(JSON.stringify(obj.nodes)) 
			};
			obj.log.nodes.forEach(n => {
				let check = Array.isArray(n.info) ? n.info[0] : n.info;
				n.type = (check && check.source) ? 'move' : 'model'; 
			});
			return obj;
		}

		function _orchestrateExecute(obj, pubApi) {
			obj.log.nodes.forEach(node => {
				console.log('running node: ' + node.name);
				node.start = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
				
				let runner = (node.type == 'move') ? pubApi.move : pubApi.model;
				
				if (Array.isArray(node.info)) {
					node.info = node.info.map(item => runner(item));
				} else {
					node.info = runner(node.info);
				}

				node.end = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
			});
			return obj;
		}

		function _orchestrateEndLog(obj) {
			obj.log.end = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
			let folderId = obj.log_destination.folder_id;
            if (folderId) {
                let folder = DriveApp.getFolderById(folderId);
                folder.createFile("log_" + obj.name + "_" + Date.now() + ".json", JSON.stringify(obj.log, null, 2), MimeType.PLAIN_TEXT);
            }
			return obj;
		}

		const api = {
			move: function(obj) { return _moveLoadData(obj, _moveGetData(obj)); },
			model: function(obj) {
				if (!obj) {
					throw new Error("O executor 'model' recebeu um objeto undefined. Verifique a configuração do seu orchestrate.");
				}
				return _pipeline(obj, _modelGetRawCode, _modelSetDependencies, (o) => { 
					o.models = _topologicalSort(o.models, "name", "depends_on"); 
					return o; 
				}, _modelCompile, _modelExecute);
			},
			orchestrate: function(obj) {
				return _pipeline(obj, _orchestrateCreateLog, (o) => { o.log.nodes = _topologicalSort(o.log.nodes, "name", "depends_on"); return o; }, (o) => _orchestrateExecute(o, api), _orchestrateEndLog);
			}
		};

		return api;
	})();

	this.tinyDeskData = tinyDeskData;
	return tinyDeskData;
}).call(this);
