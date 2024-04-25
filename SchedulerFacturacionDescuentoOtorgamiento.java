package com.banregio.sibamex.efactorskd.app.service;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.banregio.sibamex.core.service.support.SibamexService;
import com.banregio.sibamex.efactorskd.app.domain.entity.BitacoraMovimientos;
import com.banregio.sibamex.efactorskd.app.domain.entity.CalculoCesiones;
import com.banregio.sibamex.efactorskd.app.domain.entity.Calculos;
import com.banregio.sibamex.efactorskd.app.domain.entity.Cesiones;
import com.banregio.sibamex.efactorskd.app.domain.entity.Clientes;
import com.banregio.sibamex.efactorskd.app.domain.entity.ContratosFactoraje;
import com.banregio.sibamex.efactorskd.app.domain.entity.Correos;
import com.banregio.sibamex.efactorskd.app.domain.entity.CuentaCheques;
import com.banregio.sibamex.efactorskd.app.domain.entity.DatosProveedores;
import com.banregio.sibamex.efactorskd.app.domain.entity.Documentos;
import com.banregio.sibamex.efactorskd.app.domain.entity.Factorajes;
import com.banregio.sibamex.efactorskd.app.domain.entity.Firmas;
import com.banregio.sibamex.efactorskd.app.domain.entity.FirmasElectronicas;
import com.banregio.sibamex.efactorskd.app.domain.entity.Garantias;
import com.banregio.sibamex.efactorskd.app.domain.entity.HistorialFirmas;
import com.banregio.sibamex.efactorskd.app.domain.entity.HistoricoTasas;
import com.banregio.sibamex.efactorskd.app.domain.entity.LineasCreditos;
import com.banregio.sibamex.efactorskd.app.domain.entity.LineasRenovadas;
import com.banregio.sibamex.efactorskd.app.domain.entity.ParametrosCompania;
import com.banregio.sibamex.efactorskd.app.domain.entity.ParametrosContratos;
import com.banregio.sibamex.efactorskd.app.domain.entity.ParametrosFactoraje;
import com.banregio.sibamex.efactorskd.app.domain.entity.ParametrosNafin;
import com.banregio.sibamex.efactorskd.app.domain.entity.ParametrosSoporte;
import com.banregio.sibamex.efactorskd.app.domain.entity.ParametrosSpeuas;
import com.banregio.sibamex.efactorskd.app.domain.entity.Proveedores;
import com.banregio.sibamex.efactorskd.app.domain.entity.ReportesOtorgamiento;
import com.banregio.sibamex.efactorskd.app.domain.entity.RespuestaEjecucion;
import com.banregio.sibamex.efactorskd.app.domain.entity.Saldos;
import com.banregio.sibamex.efactorskd.app.domain.entity.SaldosFactoraje;
import com.banregio.sibamex.efactorskd.app.domain.entity.Tasas;
import com.banregio.sibamex.efactorskd.app.domain.entity.TransaccionesFactoraje;
import com.banregio.sibamex.efactorskd.app.domain.entity.Usuarios;
import com.banregio.sibamex.efactorskd.app.domain.entity.UsuariosCorreo;
import com.banregio.sibamex.efactorskd.app.domain.repository.BitacoraMovimientosDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.CalculoCesionesDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.CalculosIvaDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.CesionesDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ClientesDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ConfiguracionFirmasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ContratosFactorajeDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.CorreosDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.CuentaChequesDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.DocumentosDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.DocumentosUpdateDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.FactorajesDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.FirmasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.FirmasElectronicasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.GarantiasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.HistorialFirmasUpdateDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.HistoricoTasasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.LineasCreditosDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.LineasRenovadasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ParametrosCompaniaDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ParametrosContratosDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ParametrosFactorajeDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ParametrosNafinDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ParametrosSoporteDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ParametrosSpeuasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.ProveedoresDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.SaldosDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.SaldosFactorajeDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.TasasDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.TransaccionesFactorajeDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.UsuariosCorreoDAO;
import com.banregio.sibamex.efactorskd.app.domain.repository.UsuariosDAO;
import com.banregio.sibamex.efactorskd.app.remoto.dto.AdjuntosDto;
import com.banregio.sibamex.efactorskd.app.remoto.dto.CorreosDto;
import com.banregio.sibamex.efactorskd.app.remoto.service.CorreoRemotoService;
import com.banregio.sibamex.efactorskd.app.remoto.service.FirmasRemotoService;
import com.banregio.sibamex.util.Constantes;
import com.itextpdf.text.log.SysoLogger;

import lombok.extern.log4j.Log4j2;

@Log4j2
@Service("schedulerFacturacionDescuentoOtorgamiento")
public class SchedulerFacturacionDescuentoOtorgamiento extends SibamexService<RespuestaEjecucion> {
	
	@Autowired
	ParametrosNafinDAO parametrosNafinDAO;
	@Autowired
	ParametrosSpeuasDAO parametrosSpeuasDAO;
	@Autowired
	DocumentosDAO documentosDAO;
	@Autowired
	UsuariosCorreoDAO usuariosCorreoDAO;
	@Autowired
	SaldosFactorajeDAO saldosFactorajeDAO;
	@Autowired
	LineasRenovadasDAO lineasRenovadasDAO;
	@Autowired
	DocumentosUpdateDAO documentosUpdateDAO;
	@Autowired
	SaldosDAO saldosDAO;
	@Autowired
	ProveedoresDAO proveedoresDAO;
	@Autowired
	FirmasDAO firmasDAO;
	@Autowired
	HistorialFirmasUpdateDAO historialFirmasUpdateDAO;
	@Autowired
	ParametrosCompaniaDAO parametrosCompaniaDAO;
	@Autowired
	ParametrosSoporteDAO parametrosSoporteDAO;
	@Autowired
	ParametrosFactorajeDAO parametrosFactorajeDAO;
	@Autowired
	TransaccionesFactorajeDAO transaccionesFactorajeDAO;
	@Autowired
	ConfiguracionFirmasDAO configuracionFirmasDAO;
	@Autowired
	LineasCreditosDAO lineasCreditosDAO;
	@Autowired
	CesionesDAO cesionesDAO;
	@Autowired
	CuentaChequesDAO cuentaChequesDAO;
	@Autowired
	ContratosFactorajeDAO contratosFactorajeDAO;
	@Autowired
	ParametrosContratosDAO parametrosContratosDAO;
	@Autowired
	HistoricoTasasDAO historicoTasasDAO;
	@Autowired
	FirmasElectronicasDAO firmasElectronicasDAO;
	@Autowired
	TasasDAO tasasDAO;
	@Autowired
	ClientesDAO clientesDAO;
	@Autowired
	CalculosIvaDAO calculosIvaDAO;
	@Autowired
	FactorajesDAO factorajesDAO;
	@Autowired
	FirmasRemotoService firmasRemotoService;
	@Autowired
	CorreoRemotoService correoRemotoService;
	@Autowired
	UsuariosDAO usuariosDAO;
	@Autowired
	CorreosDAO correosDAO;
	@Autowired
	BitacoraMovimientosDAO bitacoraMovimientosDAO;
	@Autowired
	CalculoCesionesDAO calculoCesionesDAO;
	@Autowired
	GarantiasDAO garantiasDAO;
	@Autowired
	ResourceLoader resourceLoader;
	String logoStart64;
	String logoEfactor64;
	
	
	String notDe = Constantes.CORREO_ORIGEN_BANREGIO;
	String CORREO_PRUEBA = "daniel.carbajal@captivasys.com";
	
	public RespuestaEjecucion ejecutar() {
		
		Date hoy = new Date();
		String fechaLog = new SimpleDateFormat("yyyyMMdd").format(hoy);
		int dias = 0;
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constantes.FORMAT_FULL_TIME);
		String cobIntAnt = "1";
		String cliTipo = "2";
		List<String> notParaErr = new ArrayList<>();
		List<String> notParaOK = new ArrayList<>();
		int flag002 = 0;
		int flag003 = 0;
		String proEmail = "";
		String cliEmail = "";

		RespuestaEjecucion respuesta = new RespuestaEjecucion();
		try {
			String notTitulo = "START BanRegio Efactor Descuento Automático el día " + (new SimpleDateFormat("dd")).format(hoy) + "/" + (new SimpleDateFormat("MMMM")).format(hoy) + "/" +
					(new SimpleDateFormat("yyyy")).format(hoy) + " " + (new SimpleDateFormat(Constantes.FORMAT_HORA)).format(hoy) + " hrs";
			
			/*Estatus del servicio inicial.*/
			String bfaStatEF = "I";
			String bfaHoVaEF = "";
			int dayOfWeek = Calendar.getInstance().get(Calendar.DAY_OF_WEEK);
			String parStaApe = "";
			BigDecimal excTot = null;
			BigDecimal saldo = null;
			String errMensaj = null;
			List<Saldos> qry = null;
			String monedaErr = null;
			BigDecimal limite = BigDecimal.ZERO;
			String mensaje = "";
			String menPais = "";
			String errFactura = "";
			String fecActual = new SimpleDateFormat(Constantes.FORMAT_DATE).format(new Date());
			logoStart64 = Base64.getEncoder().encodeToString(Files.readAllBytes(resourceLoader.getResource("classpath:img/start.jpg").getFile().toPath()));
			logoEfactor64 = Base64.getEncoder().encodeToString(Files.readAllBytes(resourceLoader.getResource("classpath:img/efactor.jpg").getFile().toPath()));
			List<ReportesOtorgamiento> listArrReporteOK = new ArrayList<>();
			List<ReportesOtorgamiento> listArrReportErr = new ArrayList<>();
			List<ReportesOtorgamiento> listContactosProveedores = new ArrayList<>();
			
			List<ParametrosNafin> listaParamentrosNafin = parametrosNafinDAO.findByExample(ParametrosNafin.builder().build(), "C1");
			String notParaNeg = "";
			List<String> listaCorreoAgro = new ArrayList<>();
			
			if(!listaParamentrosNafin.isEmpty()) {
				bfaStatEF = listaParamentrosNafin.get(0).getBfaStatEF();
				bfaHoVaEF = listaParamentrosNafin.get(0).getBfaHoVaEF();
			}
			
			if (dayOfWeek == Calendar.SUNDAY || dayOfWeek == Calendar.SATURDAY) {
	            bfaStatEF = "I";
	        }
			
			if ("I".equals(bfaHoVaEF)) {
				bfaStatEF = "I";
			}
			
			List<ParametrosSpeuas> rsParamSpei = parametrosSpeuasDAO.findByExample(ParametrosSpeuas.builder().build(), "1");
			
			if(!rsParamSpei.isEmpty()) {
				parStaApe = rsParamSpei.get(0).getParStaApe();
			}
			
			if ("N".equals(parStaApe)) {
				bfaStatEF = "I";
			}
			
			if ("A".equals(bfaStatEF)) {
				/*Transaccion para obtener todas las facturas publicadas el mismo día, correspondientes a los proveedores que eligieron se descontaran las mismas el día de su publicacion.*/
				List<Documentos> listRsFactProv1 = documentosDAO.findByExample(new Documentos(), "CV");
				
				/*Transaccion para obtener todas las facturas en donde su fecha de vencimiento es igual al día actual mas los días de descuento automático reflejados en el contrato del cliente al que pertenecen.*/
				List<Documentos> listRsFactDiDe1 = documentosDAO.findByExample(new Documentos(), "CU");
				
				/* Se obtienen los correos a los que se enviara la notificacion */
				List<UsuariosCorreo> rsUsuNotOK = usuariosCorreoDAO.findByExample(new UsuariosCorreo(), "L2");
				List<UsuariosCorreo> rsUsNotErr = usuariosCorreoDAO.findByExample(new UsuariosCorreo(), "L3");
				List<UsuariosCorreo> rsUsNotNeg = usuariosCorreoDAO.findByExample(new UsuariosCorreo(), "L7");
				List<UsuariosCorreo> emailAgro = usuariosCorreoDAO.findByExample(new UsuariosCorreo(), "L8");
				
				/*Se guardan todas las direcciones en un solo registro, separado por comas, para el envío del reporte en caso que no haya errores.*/
//				String notParaOK = rsUsuNotOK.stream().map(usuarioCorreo -> usuarioCorreo.getFcnEmail().trim()).collect(Collectors.joining(","));
				notParaOK = rsUsuNotOK.stream().map(usuario -> usuario.getFcnEmail().trim()).collect(Collectors.toList());
//				notParaErr = rsUsNotErr.stream().map(usuarioCorreo -> usuarioCorreo.getFcnEmail().trim()).collect(Collectors.joining(","));
				notParaErr = rsUsNotErr.stream().map(usuario -> usuario.getFcnEmail().trim()).collect(Collectors.toList());
				notParaNeg = rsUsNotNeg.stream().map(usuarioCorreo -> usuarioCorreo.getFcnEmail().trim()).collect(Collectors.joining(","));
//				listaCorreoAgro = emailAgro.stream().map(usuarioCorreo -> usuarioCorreo.getFcnEmail().trim()).collect(Collectors.joining(","));
				listaCorreoAgro = emailAgro.stream().map(usuario -> usuario.getFcnEmail().trim()).collect(Collectors.toList());
				
				flag002 = 0;
				flag003 = 0;
				String dLinea = "";
				
				if ((listRsFactProv1 != null && !listRsFactProv1.isEmpty()) || 
					(listRsFactDiDe1 != null && !listRsFactDiDe1.isEmpty())) {
					errMensaj = "";
					
					for (Documentos rsFactProv1: listRsFactProv1) {
						/*Transacción para consultar el saldo pendiente en facturas.*/
						try {
							List<SaldosFactoraje> qryFalPago = saldosFactorajeDAO.findByExample(SaldosFactoraje.builder().linNumero(rsFactProv1.getDocLinea()).build(), "");
						
/*Revisar que pasa si*/		errMensaj = qryFalPago.get(0).getErrMensaj().trim();
/*existe error*/		} catch (Exception e) {
							errMensaj = e.getMessage();
						}
						if("".equals(errMensaj)) {
							/*Transacción para consultar el saldo pendiente en facturas.*/
							List<LineasRenovadas> qryDocLinea1 = lineasRenovadasDAO.findByExample(LineasRenovadas.builder().facLinAnt(rsFactProv1.getDocLinea()).build(), "");
							
							if (!qryDocLinea1.isEmpty()) {
								// Se modifica la línea por la que se renovó
								List<Documentos> rsActuFact1 = documentosUpdateDAO.findByExample(Documentos.builder().docProvee(rsFactProv1.getDocComPro())
																													 .docNumero(rsFactProv1.getDocNumero())
																													 .docLinea(qryDocLinea1.get(0).getFacLinRen())
																													 .tipActual("V").build(), "");
							}
						}
					}
					
					for (Documentos rsFactDiDe1: listRsFactDiDe1) {
						try {
							/*Transacción para consultar el saldo pendiente en facturas.*/
							List<SaldosFactoraje> qryFalPago = saldosFactorajeDAO.findByExample(SaldosFactoraje.builder().linNumero(rsFactDiDe1.getDocLinea()).build(), "");
							
							errMensaj = qryFalPago.get(0).getErrMensaj().trim();
						} catch (Exception e) {
							 errMensaj = e.getMessage();
						}
						if("".equals(errMensaj)) {
							/*Transacción para consultar el saldo pendiente en facturas.*/
							List<LineasRenovadas> qryDocLinea2 = lineasRenovadasDAO.findByExample(LineasRenovadas.builder().facLinAnt(rsFactDiDe1.getDocLinea()).build(), "");
							
							if (!qryDocLinea2.isEmpty()) {
								// Se modifica la línea por la que se renovó
								List<Documentos> rsActuFact1 = documentosUpdateDAO.findByExample(Documentos.builder().docProvee(rsFactDiDe1.getDocComPro())
																													 .docNumero(rsFactDiDe1.getDocNumero())
																													 .docLinea(qryDocLinea2.get(0).getFacLinRen())
																													 .tipActual("V").build(), "");
							}
						}
					}
				}
				
				/*Transaccion para obtener todas las facturas publicadas el mismo día, correspondientes a los proveedores que eligieron se descontaran las mismas el día de su publicacion.*/
				List<Documentos> rsFactProv = documentosDAO.findByExample(new Documentos(), "CV");
				
				/*Transaccion para obtener todas las facturas en donde su fecha de vencimiento es igual al día actual mas los días de descuento automático reflejados en el contrato del cliente al que pertenecen.*/
				List<Documentos> rsFactDiDe = documentosDAO.findByExample(new Documentos(), "CU");
				
				if ((rsFactProv != null && !rsFactProv.isEmpty()) || 
					(rsFactDiDe != null && !rsFactDiDe.isEmpty())) {

					/*Valida la cantidad de cesiones que se registrarán, es decir, se juntan todas las facturas  de un solo proveedor y luego se agrupan por cliente.*/
					int numProvee = 0;
					
					/*Obtiene el número de proveedores diferentes de la primer regla de negocio.*/
					Map<String, Documentos> registrosUnicos = new HashMap<>();
					for (Documentos documento : rsFactProv) {
			            String key = documento.getDocComPro() + documento.getUsfNivMan();
			            registrosUnicos.putIfAbsent(key, documento);
			        }
					
					List<Documentos> qryRsFaPr = new ArrayList<>(registrosUnicos.values());
					
					/*Actualiza el número de proveedores diferentes*/
					numProvee+=qryRsFaPr.size();
					
					/*Obtiene el número de proveedores diferentes de la segunda regla de negocio.*/
					registrosUnicos = new HashMap<>();
					for (Documentos documento : rsFactDiDe) {
			            String key = documento.getDocComPro() + documento.getUsfNivMan();
			            registrosUnicos.putIfAbsent(key, documento);
			        }
					
					List<Documentos> qryRsFaDi = new ArrayList<>(registrosUnicos.values());
					
					/*Actualiza el número de proveedores diferentes*/
					numProvee+=qryRsFaDi.size();
					
//					int regProv = 1; 
					
					/*Se guarda en una variable alterna todos los números de los proveedores juntos, esto para facilitar la codificación más adelante.*/
					List<DatosProveedores> listQryProvees = new ArrayList<>();
					for (Documentos documento : qryRsFaPr) {
						listQryProvees.add(DatosProveedores.builder().docComPro(documento.getDocComPro()).usfNivMan(documento.getUsfNivMan()).build());
//						regProv++;
					}
					
					for (Documentos documento : qryRsFaDi) {
						List<DatosProveedores> proveeCoincidentes = listQryProvees.stream().filter(provee -> provee.equals(documento.getDocComPro())).collect(Collectors.toList());
						if (proveeCoincidentes.isEmpty()) {
							listQryProvees.add(DatosProveedores.builder().docComPro(documento.getDocComPro()).usfNivMan(documento.getUsfNivMan()).build());
//							regProv++;
						}
					}
					
					/*Sabiendo el número de proveedores diferentes, se procede a obtener los clientes por cada uno.*/
					int numClientes = 0;
					List<List<String>> arrRsFaPrCli = new ArrayList<>();
					/*Se guarda en una variable alterna todos los números de los proveedores incluyendo a sus respectivos clientes.*/
					List<Documentos> qryProvCli = new ArrayList<>();
					/*Contador de registros para reporte proveedor/cliente*/
//					int intRegProCli = 0;
					
					for (Documentos documentoDist: qryRsFaPr) {
			            Set<String> qryRsFaPrCliSet = new HashSet<>();
			            for (Documentos documento: rsFactProv) {
			                if (documento.getDocComPro().equals( documentoDist.getDocComPro())) {
			                    qryRsFaPrCliSet.add(documento.getDocFactor());
			                }
			            }
			            arrRsFaPrCli.add(new ArrayList<>(qryRsFaPrCliSet));
			            
			            for (String docFactor: qryRsFaPrCliSet) {
			            	qryProvCli.add(Documentos.builder().docComPro(documentoDist.getDocComPro()).docFactor(docFactor).build());
//			            	intRegProCli++;
			            }
			        }
					
					/*Actualiza el número de clientes diferentes.*/
					numClientes+=arrRsFaPrCli.size();
					List<List<String>> arrRsFaDiCli = new ArrayList<>();
					
					for(Documentos documentoDist: qryRsFaDi) {
						Set<String> qryRsFaDiCliSet = new HashSet<>();
						for (Documentos documento: rsFactDiDe) {
			                if (documento.getDocComPro().equals( documentoDist.getDocComPro())) {
			                	qryRsFaDiCliSet.add(documento.getDocFactor());
			                }
			            }
						arrRsFaDiCli.add(new ArrayList<>(qryRsFaDiCliSet));
						
						for(String docFactor: qryRsFaDiCliSet) {
							qryProvCli.add(Documentos.builder().docComPro(documentoDist.getDocComPro()).docFactor(docFactor).build());
//			            	intRegProCli++;
						}
					}
					
					/*Actualiza el número de clientes diferentes.*/
					numClientes+=arrRsFaDiCli.size();
					
					/*En este arreglo se almacenará la información necesaria para cada cesión, con una estructura por niveles*/ 
					Map<Integer, List<Documentos>> arrCesiones = new HashMap<>();
					
					/*Por último, se toma la información de cada factura, tomando en cuenta las subdivisiones realizadas anteriormente.*/
					int numFacturas = 0;
					
					/*Se guarda en una variable alterna todos los números de los proveedores incluyendo a sus respectivos clientes.*/
					List<Documentos> listQryProCliFac = new ArrayList<>();
					List<Documentos> qryProCliFac2 = new ArrayList<>();
					List<Documentos> qryRsFaPrFac;
					List<Documentos> qryRsFaDiFac;
					String proveedor = "";
					
					for (int row = 0; row < qryRsFaPr.size(); row++) {
						 for (int col = 0; col < arrRsFaPrCli.get(row).size(); col++) {
							 int indexRow = row;
							 int indexCol = col;
							 qryRsFaPrFac = rsFactProv.stream()
											.filter(documento -> documento.getDocComPro().equals(qryRsFaPr.get(indexRow).getDocComPro())&&
																 documento.getDocFactor().equals(arrRsFaPrCli.get(indexRow).get(indexCol)))
											.collect(Collectors.toList());
						
							 qryProCliFac2.addAll(qryRsFaPrFac);
						}
					}
					
					for (int row = 0; row < qryRsFaDi.size(); row++) {
						for (int col = 0; col < arrRsFaDiCli.get(row).size(); col++) {
							int indexRow = row;
							int indexCol = col;
							qryRsFaDiFac = rsFactDiDe.stream()
										   .filter(documento -> documento.getDocComPro().equals(qryRsFaDi.get(indexRow).getDocComPro())&&
													 			documento.getDocFactor().equals(arrRsFaDiCli.get(indexRow).get(indexCol)))
										   .collect(Collectors.toList());
					
							qryProCliFac2.addAll(qryRsFaDiFac);
						}
					}
					
					for (DatosProveedores qryProvees: listQryProvees ) {						
						int count = 0;
						int flag001 = 0;
						flag003 = 0;
						int valPais = 0;
//						int intCliente = 0;
						String oldclient = "";
						Integer contArrCesiones = 0;
						arrCesiones.put(contArrCesiones, new ArrayList<>());
						
						proveedor = qryProvees.getDocComPro();	
						
						List<Documentos> listProvees2  = qryProCliFac2.stream()
				                .filter(documento -> documento.getDocComPro().equals(qryProvees.getDocComPro()))
				                .collect(Collectors.toList());
						
						listProvees2 = ordenarLista(listProvees2,
				                Comparator.comparing(Documentos::getCliNumero).reversed(),
				                Comparator.comparing(Documentos::getDocContra),
				                Comparator.comparing(Documentos::getDocLinea),
				                Comparator.comparing(Documentos::getDocCosto),
				                Comparator.comparing(Documentos::getDocMoneda),
				                Comparator.comparing(Documentos::getDocFecVen),
				                Comparator.comparing(Documentos::getDocCantid).reversed());
						
						for (Documentos qryProvees2: listProvees2) {
							if (count == 0) {
								oldclient = qryProvees2.getCliNumero();
//								intCliente = 1;
								excTot = BigDecimal.ZERO;

								qry = saldosDAO.findByExample(Saldos.builder().numComPro(proveedor)
							  												  .tipDeudor("P")
																			  .tipMoneda(qryProvees2.getDocMoneda())
																			  .numClient(qryProvees2.getCliNumero())
																			  .numContra(qryProvees2.getDocContra())
																			  .numLinea(qryProvees2.getDocLinea()).build(), "");
								saldo = new BigDecimal(qry.get(0).getMonLimite());
								saldo = saldo.subtract(new BigDecimal(qry.get(0).getMonSaldo()));
							}
							
							if (!oldclient.equals(qryProvees2.getCliNumero())) {
								oldclient = qryProvees2.getCliNumero();
//								intCliente++;
								excTot = BigDecimal.ZERO;
							
								qry = saldosDAO.findByExample(Saldos.builder().numComPro(proveedor)
										   .tipDeudor("P")
										   .tipMoneda(qryProvees2.getDocMoneda())
										   .numClient(qryProvees2.getCliNumero())
										   .numContra(qryProvees2.getDocContra())
										   .numLinea(qryProvees2.getDocLinea()).build(), "");
								
								saldo = new BigDecimal(qry.get(0).getMonLimite());
								saldo = saldo.subtract(new BigDecimal(qry.get(0).getMonSaldo()));
								
							}
							
							try {
								List<SaldosFactoraje> qryFalPago = saldosFactorajeDAO.findByExample(SaldosFactoraje.builder().linNumero(qryProvees2.getDocLinea()).build(), "");
							
								errMensaj = qryFalPago.get(0).getErrMensaj().trim();
							} catch (Exception e) {
								errMensaj = e.getMessage();
							}
							
							if ("".equals(errMensaj)) {
								count++;
								
								if (count == 1) {
									if (Integer.valueOf(qryProvees2.getDocMoneda()) == 2) {
										valPais = 0;
										/*Transacción para consolidar el requisito de firmas en la factura.*/
										List<Proveedores> rsProPai = proveedoresDAO.findByExample(Proveedores.builder().proNumero(proveedor).build(), "C1");
										
										if (!rsProPai.get(0).getProNoBaBe().trim().isBlank() && 
											rsProPai.get(0).getProPais().trim().isBlank()) {
											valPais = 1;
										}
									}
								}
							
								if (valPais == 0) {
									String tipoCosto = "";
									if ("E".equals(qryProvees2.getDocCosto().trim())) {
										tipoCosto = "Emisor";
									} else {
										tipoCosto = "Proveedor";
									}

									if (saldo.compareTo(BigDecimal.ZERO)>=0) {											
										saldo = saldo.subtract((new BigDecimal(qryProvees2.getDocCantid())).multiply(new BigDecimal(qry.get(0).getAforo())).divide(new BigDecimal("100")));
										if (saldo.compareTo(BigDecimal.ZERO)>=0) {		
											
											Documentos documento = qryProvees2;
											arrCesiones.get(contArrCesiones).add(documento);
											
											listQryProCliFac.add(documento);
										} else {
											excTot = excTot.add((new BigDecimal(qryProvees2.getDocCantid())).multiply(new BigDecimal(qry.get(0).getAforo())).divide(new BigDecimal("100")));
											monedaErr =  qryProvees2.getDocMoneda();
											saldo = saldo.add((new BigDecimal(qryProvees2.getDocCantid())).multiply(new BigDecimal(qry.get(0).getAforo())).divide(new BigDecimal("100")));
											limite = excTot.subtract(saldo);
//											
											if (flag001 == 0) {
												mensaje+=Constantes.STR_NO_PROVEEDOR + proveedor + ' ' + qryProvees2.getProComple() + ", Tipo de Costo " + tipoCosto + " excede su límite. ";
												flag001 = 1;
											}
										}
									} else {
										excTot = excTot.add((new BigDecimal(qryProvees2.getDocCantid())).multiply(new BigDecimal(qry.get(0).getAforo())).divide(new BigDecimal("100")));
										monedaErr =  qryProvees2.getDocMoneda();
										saldo = saldo.add((new BigDecimal(qryProvees2.getDocCantid())).multiply(new BigDecimal(qry.get(0).getAforo())).divide(new BigDecimal("100")));
										limite  = excTot.subtract(saldo);

										if (flag001 == 0) {
											mensaje+=Constantes.STR_NO_PROVEEDOR + proveedor + ' ' + qryProvees2.getProComple() + ", Tipo de Costo " + tipoCosto + " excede su límite. ";
											flag001 = 1;

										}
									}
								} else {
									if (flag003 == 0) {
										menPais+=Constantes.STR_NO_PROVEEDOR + proveedor  + " no tiene asignado un pais para otorgamiento en dolares ";
										flag003 = 1;
									}
								}
							} else {							
								if (flag002 == 0) {
									flag002 = 1;
									errFactura = "Las siguientes lineas tienen Documentos Pendientes de Pago <br>";
								}
								if (!qryProvees2.getDocLinea().equals(dLinea)) {
									dLinea = qryProvees2.getDocLinea();
									errFactura+="Cliente: " + qryProvees2.getCliNumero() + " Linea: " + qryProvees2.getDocLinea() + " <br>";
								}
							}
						}
						
						if (flag001 == 1 && !"".equals(mensaje)) {
							if ("01".equals(monedaErr)) {
								monedaErr = Constantes.STR_PESOS;
							} else {
								monedaErr = Constantes.STR_DOLARES;
							}
							
							mensaje+=NumberFormat.getCurrencyInstance(Locale.US).format(limite) + " " + monedaErr + " <br>";
							excTot = BigDecimal.ZERO;
						}
						contArrCesiones++;
					}

//error					for (int idarr=0 ; idarr<arrCesiones.size() ; idarr++) {
//						if (arrCesiones.get(idarr).isEmpty()) {
//							arrCesiones.remove(idarr);						
//						}
//					}
					
					/*Se realizan las validaciones necesarias para generar el alta de la cesión*/
					/*Variable para indicar en que numero de factura de todas las que se estan procesando va la validacion.*/
					int intRegProCliFac = 0; 
					
					/*Proceso para revisar el número de firmas de cada documento.*/
					
					for (int intPosPriDim=0 ; intPosPriDim<arrCesiones.size() ; intPosPriDim++) {
						
						/*Contador de firmas por cesiones de proveedor.*/
						int intContFirm = 0;
						/*Bandera para indicar que hubo un resultado positivo en la consulta de firmas.*/
						String chrBandFirm = "N";
						/*Variable que guarda la información de las firmas por factura.*/
						String intMemFirma = "";
						
						for (Documentos documento : arrCesiones.get(intPosPriDim)) {
							/*Transacción para consolidar el requisito de firmas en la factura.*/
							List<Firmas>rsFactFirm = firmasDAO.findByExample(Firmas.builder().firUsuari(documento.getConTipLin())
																							.firFolio(documento.getDocFolio())
																							.firProvee(documento.getDocComPro()).build(), proveedor);
							
							if (rsFactFirm != null && !rsFactFirm.isEmpty()) {
								chrBandFirm = "S";
							}
							
							if (!intMemFirma.equals(documento.getDocConFir()) && !intMemFirma.isEmpty()) {
								intContFirm = intContFirm + 1;
								intMemFirma = documento.getDocConFir();
							} else  if (intMemFirma.isEmpty()) {
								intContFirm = intContFirm + 1;
								intMemFirma = documento.getDocConFir();
							}
						}
						
						/*Registra que la cesión de este proveedor tuvo un problema con las firmas del documento, 
						revisando que el número de estas sea igual en todos los documentos, así como que este no sea menor al nivel del usuario.*/
						if (intContFirm != 1) {
							listQryProvees.get(intPosPriDim).setErrFirmas(1);
						} else if ("S".equals(chrBandFirm)) {
							listQryProvees.get(intPosPriDim).setErrFirmas(2);
						/*Valida que el número de firmas sea mayor al nivel del proveedor.*/
//						}else if(intMemFirma + 1 LT qryProvees['Usf_NivMan'][intPosPriDim]) {
						} else if (Integer.valueOf(intMemFirma) + 1 < Integer.valueOf(listQryProvees.get(intPosPriDim).getUsfNivMan())) {
							for (Documentos documento : arrCesiones.get(intPosPriDim)) {
								/*Transacción para actualizar el estatus de las facturas de esta cesión.*/
								documentosUpdateDAO.findByExample(Documentos.builder().docProvee(documento.getDocComPro())
								 																			 .docNumero(documento.getDocNumero())
								 																			 .docCantid("0")
								 																			 .docFirmas((Integer.valueOf(intMemFirma) + 1)+"")
								 																			 .tipActual("A").build(), "");
								/*Transacción para registrar las firmas correspondientes.*/
								historialFirmasUpdateDAO.findByExample(HistorialFirmas.builder().firUsuari(documento.getConTipLin())
																								.firFolio(documento.getDocFolio())
																								.firProvee(documento.getDocComPro())
																								.firFecha((new Date()).getTime() + "").build(), "");
							}
							listQryProvees.get(intPosPriDim).setErrFirmas(3);
						}else {
							listQryProvees.get(intPosPriDim).setErrFirmas(0);
						}
						/*Registra el estatus de cada factura para esta cesión, de acuerdo a lo validado anteriormente.*/
						if(listQryProvees.get(intPosPriDim).getErrFirmas()==0) {
							for (int intPosSegDim = 0 ; intPosSegDim<arrCesiones.get(intPosPriDim).size() ; intPosSegDim++) {
								listQryProCliFac.get(intRegProCliFac).setErrFirmas(0);
								intRegProCliFac++;
							}
						} else {
							switch (listQryProvees.get(intPosPriDim).getErrFirmas()) {
								case 1:
									for (int intPosSegDim = 0 ; intPosSegDim<arrCesiones.get(intPosPriDim).size() ; intPosSegDim++) {
										listQryProCliFac.get(intRegProCliFac).setErrFirmas(1);
										intRegProCliFac++;
									}
								break;
								case 2:
									for (int intPosSegDim = 0 ; intPosSegDim<arrCesiones.get(intPosPriDim).size() ; intPosSegDim++) {
										listQryProCliFac.get(intRegProCliFac).setErrFirmas(2);
										intRegProCliFac++;
									}
								break;	
								case 3:
									for (int intPosSegDim = 0 ; intPosSegDim<arrCesiones.get(intPosPriDim).size() ; intPosSegDim++) {
										listQryProCliFac.get(intRegProCliFac).setErrFirmas(3);
										intRegProCliFac++;
									}
								break;	
								default:
									for (int intPosSegDim = 0 ; intPosSegDim<arrCesiones.get(intPosPriDim).size() ; intPosSegDim++) {
										listQryProCliFac.get(intRegProCliFac).setErrFirmas(4);
										intRegProCliFac++;
									}
								break;
							}
						}
						/*Fin de For de Cesiones*/
					}
					
					/*Variable para indicar en qué número de factura (de todas las que se están procesando) va la validación.*/
					intRegProCliFac = 0; 
					
//					List <String> arrResAltOto;
//					String strResAltOto = "";
					
					/*Transacción para consultar clabe de cuenta ordenante comp Banco.*/
					List<ParametrosCompania> qryBanco = parametrosCompaniaDAO.findByExample(ParametrosCompania.builder().paaTipPro("3")
																														.paaCompan("001").build(), "C6");
					
					List<ParametrosCompania> qryAF = parametrosCompaniaDAO.findByExample(ParametrosCompania.builder().paaTipPro("3")
																													 .paaCompan("500").build(), "C6");
					/*Transacción para consultar el proveedor.*/
					List<ParametrosSoporte> qrySParams = parametrosSoporteDAO.findByExample(ParametrosSoporte.builder().parSucurs("001").build(),"C1");
					
					List<ParametrosFactoraje> qryFParams = parametrosFactorajeDAO.findByExample(ParametrosFactoraje.builder().build(),"");
					
					/*Ciclo para validar la capacidad de pago por cesión.*/
					/*Ciclo de proveedores*/
/*Error*/					for (int intRegist=0 ; intRegist<arrCesiones.size() ; intRegist++) {
						/*Sólo revisa las cesiones cuando no hubo errores de firmas en las facturas.*/
/*Error*/						if(listQryProvees.get(intRegist).getErrFirmas() == 0) {
							
							proveedor = arrCesiones.get(intRegist).get(0).getDocComPro();
							
							List<Proveedores> qryProvee = proveedoresDAO.findByExample(Proveedores.builder().proNumero(proveedor).build(), "C1");
							
							/*Revisa las facturas de cada cliente del actual proveedor, para confirmar específicamente que el saldo de la línea de crédito de cada cliente
							tenga los suficientes fondos para cubrir el factoraje.*/
							/*Ciclo de Clientes por proveedor*/
							
							BigDecimal monto = BigDecimal.ZERO;
							String docContra = "";
							String docLinea = "";
							String docCosto = "";
							String docMoneda = "";
							String docContraAnt = "";
							String docLineaAnt = "";
							String docCostoAnt = "";
							String docMonedaAnt = "";
							String aplicaIVA = "N";
							
							/*Transacción para obtener el número de movimiento.*/
							List<TransaccionesFactoraje> transacciones = transaccionesFactorajeDAO.findByExample(TransaccionesFactoraje.builder().build(), "1");
							
							/*Guarda el número del movimiento.*/
							String docMovimi = transacciones.get(0).getFolTransa();
							String tasaAnt = "";
							BigDecimal dblTotal	= BigDecimal.ZERO;
							String moneda = "";
							
							/*Transacción para consultar la configuración de Firmas Elec. Spei*/
//							List<ConfiguracionFirmas> conFirmas1 = configuracionFirmasDAO.findByExample(ConfiguracionFirmas.builder().cfeModulo("SP").build(), "C1");
//							List<ConfiguracionFirmas> conFirmas2 = configuracionFirmasDAO.findByExample(ConfiguracionFirmas.builder().cfeModulo("SD").build(), "C1");
							String cuentaOrdenante = "";
							String cueDest = "";
							String firmaElectronica = "";
							String firCliente = "";
							String firProveed = "";
							String firContrat	= "";
							String firLinea	= "";
							String firCompan = "";
							
							/*Ciclo de documentos de cliente por proveedor*/
							for (int intPosTerDim=0 ; intPosTerDim<arrCesiones.get(intRegist).size() ; intPosTerDim++) {
								moneda = arrCesiones.get(intRegist).get(intPosTerDim).getDocMoneda();
								BigDecimal dblLinLinea = BigDecimal.ZERO;
								
								/*Variable que almacenará el monto pendiente del cliente.*/
								BigDecimal dblMontPend = BigDecimal.ZERO;
								
								/*Transacción para consultar el saldo de la línea.*/
								List<LineasCreditos> rsLineaCre = lineasCreditosDAO.findByExample(LineasCreditos.builder().linNumero(arrCesiones.get(intRegist).get(intPosTerDim).getDocLinea()).build(), "C1");
								
								/*Transacción para consultar el saldo pendiente en facturas.*/
								List<Cesiones> rsMontPend = cesionesDAO.findByExample(Cesiones.builder().conClient(arrCesiones.get(intRegist).get(intPosTerDim).getDocLinea().substring(0, 8))
																															  .facMonto("0")
																															  .facCantid(dblMontPend + "")
																															  .build(), "C1");	
								
								/*Se registra la diferencia entre el saldo de la línea menos el monto pendiente.*/
								if (rsMontPend != null && !rsMontPend.isEmpty()) {
									dblLinLinea = (new BigDecimal(rsLineaCre.get(0).getLinSaldo())).subtract(new BigDecimal(rsMontPend.get(0).getFacCantid()));
								} else {
									dblLinLinea = new BigDecimal(rsLineaCre.get(0).getLinSaldo());
								}
								/*Hace la suma o la resta de la factura a la cesión, dependiendo del tipo de documento.*/
								
								if ("03".equals(arrCesiones.get(intRegist).get(intPosTerDim).getDocTipDoc())) {
									dblTotal = dblTotal.subtract(new BigDecimal(arrCesiones.get(intRegist).get(intPosTerDim).getDocCantid()));
								} else if ("01".equals(arrCesiones.get(intRegist).get(intPosTerDim).getDocTipDoc())) {
									dblTotal = dblTotal.add(new BigDecimal(arrCesiones.get(intRegist).get(intPosTerDim).getDocCantid()));
								} 

								if((dblLinLinea.subtract(dblTotal).compareTo(BigDecimal.ZERO)) >= 0) {
									if ("01".equals(arrCesiones.get(intRegist).get(intPosTerDim).getDocTipDoc())) {
										/*Transacción para actualizar el estatus de las facturas de esta cesión.*/
										documentosUpdateDAO.findByExample(Documentos.builder().docProvee(arrCesiones.get(intRegist).get(intPosTerDim).getDocComPro())
																							 .docNumero(arrCesiones.get(intRegist).get(intPosTerDim).getDocNumero())
																							 .docMovimi(docMovimi)
																							 .docCantid("0")
																							 .docFirmas("0")
																							 .docFecha((new Date().getTime()) + "")
																							 .tipActual("B").build(), "");
										/*Cuando el documento es nota de crédito, se pasan las fechas de vencimiento y pago.*/
									} else if ("03".equals(arrCesiones.get(intRegist).get(intPosTerDim).getDocTipDoc())) {
										/*Transacción para actualizar el estatus de las notas de crédito de esta cesión.*/
										documentosUpdateDAO.findByExample(Documentos.builder().docProvee(arrCesiones.get(intRegist).get(intPosTerDim).getDocComPro())
																							.docNumero(arrCesiones.get(intRegist).get(intPosTerDim).getDocNumero())
																							.docMovimi(docMovimi)
																							.docCantid("0")
																							.docFirmas("0")
																							.docFecha((new Date().getTime()) + "")
																							.docFecVen((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecVen()).getTime() + "")
																							.docFecVen((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecPag()).getTime() + "")
																							.docFecPag(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecPag())
																							.tipActual("H").build(), "");
									}
										
									/*Registra que sí hubo fondos suficientes en la línea.*/
									listQryProvees.get(intRegist).setErrDesFac(0);
									listQryProCliFac.get(intRegProCliFac).setErrDesFac(0);

									/*Actualiza el número de factura (renglón en la consulta qryProCliFac).*/
									intRegProCliFac++;			
									
									/*Movimientos para generar la firma electrónica*/
									if ("01".equals(moneda.trim()) && qryProvee.get(0).getProCuePes().trim().isEmpty() ||
										"02".equals(moneda.trim()) && qryProvee.get(0).getProCueDol().trim().isEmpty()) {
										
											/*Validaciones y consultas para firma electrónica*/
										firProveed = arrCesiones.get(intRegist).get(intPosTerDim).getDocComPro();
										firCliente = arrCesiones.get(intRegist).get(intPosTerDim).getDocLinea().substring(0, 8);
										firContrat	= arrCesiones.get(intRegist).get(intPosTerDim).getDocContra();
										firLinea	= arrCesiones.get(intRegist).get(intPosTerDim).getDocLinea();
										
										List<Documentos> qryDocume = documentosDAO.findByExample(Documentos.builder().docProvee(firProveed)
																													 .docNumero(arrCesiones.get(intRegist).get(intPosTerDim).getDocNumero()).build(), "CO");
										
										firCompan = qryDocume.get(0).getDocCompan();
										
										String cueOrde = "";
										
										if ("01".equals(moneda.trim())) {
											if ("001".equals(qryDocume.get(0).getDocCompan())) {
												cueOrde = qryBanco.get(0).getPaaCuePes();
											} else if ("500".equals(qryDocume.get(0).getDocCompan())) {
												cueOrde = qryAF.get(0).getPaaCuePes();
											} else {
												cueOrde = "";
											}
										} else if ("02".equals(moneda.trim())) {
											if ("001".equals(qryDocume.get(0).getDocCompan())) {
												cueOrde = qryBanco.get(0).getPaaCueDol();
											} else if ("500".equals(qryDocume.get(0).getDocCompan())) {
												cueOrde = qryAF.get(0).getPaaCueDol();
											} else {
												cueOrde = "";
											}
										}

										if (!cueOrde.isEmpty()) {
											/*Transacción para consultar la cuenta ordenante.*/
											List<CuentaCheques> listCuentaOrdenante = cuentaChequesDAO.findByExample(CuentaCheques.builder().cueNumero(cueOrde).build(), "CI");
											
											cuentaOrdenante = listCuentaOrdenante!=null && !listCuentaOrdenante.isEmpty() ? listCuentaOrdenante.get(0).getCueClabe() : "";
										}
										
										
										
										if ("01".equals(moneda.trim())) {
											cueDest = qryProvee.get(0).getProClaSpe();
										} else {
											cueDest = qryProvee.get(0).getProClaSpi();
										}
										
										List<ContratosFactoraje> qryContra = contratosFactorajeDAO.findByExample(ContratosFactoraje.builder().conNumero(arrCesiones.get(intRegist).get(intPosTerDim).getDocContra()).build(), "C7");
										List<ParametrosContratos> qryParCon = parametrosContratosDAO.findByExample(ParametrosContratos.builder().pcfContra(arrCesiones.get(intRegist).get(intPosTerDim).getDocContra()).build(), "C1");
										List<HistoricoTasas> qryTasa = historicoTasasDAO.findByExample(HistoricoTasas.builder().tipo(qryContra.get(0).getConTasBas())
																															   .fecha((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(qrySParams.get(0).getParFecAct()).getTime() + "")
																															   .tasa(qryContra.get(0).getConTasa()).build(), "");
										
										BigDecimal totalDoc = new BigDecimal(qryDocume.get(0).getDocCantid()).subtract(new BigDecimal(qryContra.get(0).getConHonNot())).subtract(new BigDecimal(qryContra.get(0).getConIvHoNo())).subtract(new BigDecimal(qryContra.get(0).getConGasReg())).subtract(new BigDecimal(qryContra.get(0).getConIvGaRe())).subtract(new BigDecimal(qryContra.get(0).getConCobAva())).subtract(new BigDecimal(qryContra.get(0).getConIvCoAv()));
										
										String tipFoTaFi = "001";
										BigDecimal canIntIVA = BigDecimal.ZERO;
										BigDecimal faTasa;
										BigDecimal conTasSob;
										BigDecimal conTasFac;

										if (qryTasa.get(0).getTasa().isEmpty()) {
											faTasa = BigDecimal.ZERO;
										} else {
											faTasa = new BigDecimal(qryTasa.get(0).getTasa());
										}
										if (qryContra.get(0).getConTasSob().isEmpty()) {
											conTasSob = BigDecimal.ZERO;
										} else {
											conTasSob = new BigDecimal(qryContra.get(0).getConTasSob());
										}
										if (qryContra.get(0).getConTasFac().isEmpty()) {
											conTasFac = BigDecimal.ZERO;
										} else {
											conTasFac = new BigDecimal(qryContra.get(0).getConTasFac());
										}
										
										BigDecimal sobTasa = faTasa.add(conTasSob);
										BigDecimal mulTasa = faTasa.multiply(conTasFac);
										
										
										if (!"001".equals(qryContra.get(0).getConFormul())) {
											if(sobTasa.compareTo(mulTasa) >= 0) {
												faTasa = sobTasa;
											} else {
												faTasa = mulTasa;
											}
										}
										
										docContra = arrCesiones.get(intRegist).get(intPosTerDim).getDocContra();
										docLinea = arrCesiones.get(intRegist).get(intPosTerDim).getDocLinea();
										docCosto = arrCesiones.get(intRegist).get(intPosTerDim).getDocCosto();
										docMoneda = arrCesiones.get(intRegist).get(intPosTerDim).getDocMoneda();
										
										
										
										
										if (docContraAnt.isEmpty()) {
											docContraAnt = docContra;
										}
										if (docLineaAnt.isEmpty()) {
											docLineaAnt = docLinea;
										}
										if (docCostoAnt.isEmpty()) {
											docCostoAnt = docCosto;
										}
										if (docMonedaAnt.isEmpty()) {
											docMonedaAnt = docMoneda;
										}
										
										if (!docContra.equals(docContraAnt) || !docLinea.equals(docLineaAnt) || !docCosto.equals(docCostoAnt) || !docMoneda.equals(docMonedaAnt)) {
											if (monto.compareTo(BigDecimal.ZERO) > 0) {
												String proveedorAnt = arrCesiones.get(intRegist).get(intPosTerDim-1).getDocComPro();
												
												List<Documentos> qryDocumeAnt = documentosDAO.findByExample(Documentos.builder().docProvee(proveedorAnt)
																																.docNumero(arrCesiones.get(intRegist).get(intPosTerDim-1).getDocNumero()).build(), "CO");
												
												List<Proveedores> qryProveeAnt = proveedoresDAO.findByExample(Proveedores.builder().proNumero(proveedorAnt).build(), "C1");
												
//												String firmIP = "0";
//												String firmPuerto = "0";
												String cueDestAnt;
												String cueOrdeAnt;
												String modulo;
												if ("01".equals(docMonedaAnt)) {
//													firmIP = conFirmas1.get(0).getCfeIpBaEl();
//													firmPuerto = conFirmas1.get(0).getCfePuBaEl();
													modulo = "SP";
													cueDestAnt = qryProveeAnt.get(0).getProClaSpe();
													if ("001".equals(qryDocumeAnt.get(0).getDocCompan())) {
														cueOrdeAnt = qryBanco.get(0).getPaaCuePes();
													} else if ("500".equals(qryDocumeAnt.get(0).getDocCompan())) {
														cueOrdeAnt = qryAF.get(0).getPaaCuePes();
													} else {
														cueOrdeAnt = "";
													}
												} else {
//													firmIP = conFirmas2.get(0).getCfeIpBaEl();
//													firmPuerto = conFirmas2.get(0).getCfePuBaEl();
													modulo = "SD";
													cueDestAnt = qryProveeAnt.get(0).getProClaSpi();
													if ("001".equals(qryDocumeAnt.get(0).getDocCompan())) {
														cueOrdeAnt = qryBanco.get(0).getPaaCueDol();
													} else if ("500".equals(qryDocumeAnt.get(0).getDocCompan())) {
														cueOrdeAnt = qryAF.get(0).getPaaCueDol();
													} else {
														cueOrdeAnt = "";
													}
												}
												
												String cuentaOrdenanteAnt = "";
												
												if (!cueOrdeAnt.isEmpty()) {
													cuentaOrdenanteAnt = cuentaChequesDAO.findByExample(CuentaCheques.builder().cueNumero(cueOrdeAnt).build(), "CI").get(0).getCueClabe();
													
												}
												
												if (cuentaOrdenanteAnt.trim().length() == 18 && cueDestAnt.trim().length() == 18) {
													cuentaOrdenanteAnt	= cuentaOrdenanteAnt.trim();
													cueDestAnt	= cueDestAnt.trim();
													
													Date fechaFirma =  new Date();
													String fechaFirmaElectronica = (new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_2)).format(fechaFirma);		
													
													try {
														
														firmaElectronica = firmasRemotoService.crearFirma(cueDestAnt, cuentaOrdenanteAnt, fechaFirmaElectronica, modulo, (new DecimalFormat("#.00")).format(monto)).getResultset().getFirmaElectronica();
														log.info(Constantes.STR_DATOS_PARA_FIRMA + cuentaOrdenanteAnt + " " + cueDestAnt + " " + (new DecimalFormat(Constantes.FORMAT_CURRENCY)).format(monto) + " " + 
																														(new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_3)).format(fechaFirma) + "," + fechaLog + Constantes.STR_FAFIRMAELECTRONICA);
													} catch (Exception e) {
														log.error(Constantes.STR_OCURRIO_ERROR + docMovimi + ","+ fechaLog, e.getMessage());														
													}
													
													/*Transacción para guardar la firma electrónica en tabla otorgamiento.*/
													firmasElectronicasDAO.findByExample(FirmasElectronicas.builder().fieFirma(firmaElectronica) 
																													.fieFecha(fechaFirma.getTime() + "")
																													.fieClient(firCliente)
																													.fieProvee(proveedor)
																													.fieContra(docContraAnt)
																													.fieLinea(docLineaAnt)
																													.fieMonto(monto.toString())
																													.fieCompan(firCompan)
																													.docMovimi(docMovimi)
																													.fieMoneda(docMonedaAnt)
																													.fieCosto(docCostoAnt).build(),  "");

												}													
											}
											monto = BigDecimal.ZERO;
											docContraAnt = docContra; 
											docLineaAnt = docLinea;
											docCostoAnt = docCosto;
											docMonedaAnt = docMoneda;
										}
										
										if ("02".equals(moneda)) {
											dias = Math.abs(LocalDate.parse(qrySParams.get(0).getParFecAct(), formatter).
																				until(LocalDate.parse(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecVen(), formatter)).getDays());
											
											/*Transacción para consultar el proveedor.*/
											String tipoTasa = tasasDAO.findByExample(Tasas.builder().fatDias(dias + "").build(), "").get(0).getFatDias();
											
											if (tasaAnt.isEmpty()) {
												tasaAnt = tipoTasa;
											}
											
											/*Transacción para consultar el proveedor.*/
											qryTasa = historicoTasasDAO.findByExample(HistoricoTasas.builder().tipo(tipoTasa)
																											  .fecha((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(qrySParams.get(0).getParFecAct()).getTime() + "")
													   														  .tasa(qryContra.get(0).getConTasa()).build(), "");
											
											sobTasa = (new BigDecimal(qryTasa.get(0).getTasa())).add(new BigDecimal(qryContra.get(0).getConTaSoDo()));
											mulTasa = (new BigDecimal(qryTasa.get(0).getTasa()).multiply(new BigDecimal(qryContra.get(0).getConTaFaDo())));
											
											if (!qryContra.get(0).getConFormul().equals(tipFoTaFi)) {
												if (sobTasa.compareTo(mulTasa) >= 0) {
													faTasa = sobTasa;
												} else {
													faTasa = mulTasa;
												}
											}
											
											if (!tasaAnt.equals(tipoTasa)) {
												if (monto.compareTo(BigDecimal.ZERO)>0) {
													String proveedorAnt = arrCesiones.get(intRegist).get(intPosTerDim-1).getDocComPro();
													
													List<Documentos> qryDocumeAnt = documentosDAO.findByExample(Documentos.builder().docProvee(proveedorAnt)
															 																		.docNumero(arrCesiones.get(intRegist).get(intPosTerDim-1).getDocNumero()).build(), "CO");
													
													List<Proveedores> qryProveeAnt = proveedoresDAO.findByExample(Proveedores.builder().proNumero(proveedorAnt).build(), "C1");
//													String firmIP;
//													String firmPuerto;
													String cueOrdeAnt;
													String cueDestAnt;
													String modulo;
													if ("01".equals(docMonedaAnt)) {
//														firmIP = conFirmas1.get(0).getCfeIpBaEl();
//														firmPuerto = conFirmas1.get(0).getCfePuBaEl();
														modulo = "SP";
														cueDestAnt = qryProveeAnt.get(0).getProClaSpe();
														
														if ("001".equals(qryDocumeAnt.get(0).getDocCompan())) {
															cueOrdeAnt = qryBanco.get(0).getPaaCuePes();
														} else if ("500".equals(qryDocumeAnt.get(0).getDocCompan())) {
															cueOrdeAnt = qryAF.get(0).getPaaCuePes();
														} else {
															cueOrdeAnt = "";
														}
													} else {
//														firmIP = conFirmas2.get(0).getCfeIpBaEl();
//														firmPuerto = conFirmas2.get(0).getCfePuBaEl();
														modulo = "SD";
														cueDestAnt = qryProveeAnt.get(0).getProClaSpi();
														if ("001".equals(qryDocumeAnt.get(0).getDocCompan())) {
															cueOrdeAnt = qryBanco.get(0).getPaaCueDol();
														} else if ("500".equals(qryDocumeAnt.get(0).getDocCompan())) {
															cueOrdeAnt = qryAF.get(0).getPaaCueDol();
														} else {
															cueOrdeAnt = "";
														}
													}
													
													String cuentaOrdenanteAnt = "";
													
													if (cueOrdeAnt.isEmpty()) {
														List<CuentaCheques> qryClaOrdAnt  = cuentaChequesDAO.findByExample(CuentaCheques.builder().cueNumero(cueOrdeAnt).build(), "CI");
														cuentaOrdenanteAnt = qryClaOrdAnt.get(0).getCueClabe();
													}
													
													if (cuentaOrdenanteAnt.trim().length() == 18 && cueDestAnt.trim().length()  == 18) {
														cuentaOrdenanteAnt	= cuentaOrdenanteAnt.trim();
														cueDestAnt	= cueDestAnt.trim();
														
														Date fechaFirma =  new Date();
														String fechaFirmaElectronica = (new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_2)).format(fechaFirma);	
														
														try {
															
															firmaElectronica = firmasRemotoService.crearFirma(cueDestAnt, cuentaOrdenanteAnt, fechaFirmaElectronica, modulo, (new DecimalFormat("#.00")).format(monto)).getResultset().getFirmaElectronica();
															log.info(Constantes.STR_DATOS_PARA_FIRMA + cuentaOrdenanteAnt + " " + cueDestAnt + " " + (new DecimalFormat(Constantes.FORMAT_CURRENCY)).format(monto) + " " + 
																															(new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_3)).format(fechaFirma) + "," + fechaLog + Constantes.STR_FAFIRMAELECTRONICA);
														} catch (Exception e) {
															log.error(Constantes.STR_OCURRIO_ERROR + docMovimi + ","+ fechaLog, e.getMessage());														
														}
														
														
														/*Transacción para guardar la firma electrónica en tabla otorgamiento.*/
														firmasElectronicasDAO.findByExample(FirmasElectronicas.builder().fieFirma(firmaElectronica) 
																														.fieFecha(fechaFirma.getTime() + "")
																														.fieClient(firCliente)
																														.fieProvee(proveedor)
																														.fieContra(docContraAnt)
																														.fieLinea(docLineaAnt)
																														.fieMonto(monto.toString())
																														.fieCompan(firCompan)
																														.docMovimi(docMovimi)
																														.fieMoneda(docMonedaAnt)
																														.fieCosto(docCostoAnt).build(),  "");
													}
													
													docContraAnt = docContra; 
													docLineaAnt = docLinea;
													docCostoAnt = docCosto;
													docMonedaAnt = docMoneda;
													
												}
												tasaAnt	= tipoTasa;
												monto = BigDecimal.ZERO;

											}
											
											
										}
										
										BigDecimal cantidad = totalDoc.multiply((new BigDecimal(qryParCon.get(0).getPcfAforo())).divide(new BigDecimal(100)));
										dias = Math.abs(LocalDate.parse(qrySParams.get(0).getParFecAct(), formatter).
																			until(LocalDate.parse(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecVen(), formatter)).getDays());
										
										BigDecimal interes = (cantidad.multiply(new BigDecimal(dias).multiply(faTasa)));
										interes = interes.divide(new BigDecimal(qrySParams.get(0).getParDiBaCr()), 10, RoundingMode.HALF_UP).divide(new BigDecimal(100), 2, RoundingMode.HALF_UP);
										List<Calculos> ivaDocumento = null;
										
										if (qryContra.get(0).getConCobInt().equals(cobIntAnt) && "S".equals(aplicaIVA)) {
											List<Clientes> clcliente = clientesDAO.findByExample(Clientes.builder().cliNumero(arrCesiones.get(intRegist).get(intPosTerDim).getDocLinea().substring(0, 8)).build(), "C2");
											
											if (clcliente.get(0).getCliTipo().equals(cliTipo)) {
												ivaDocumento = calculosIvaDAO.findByExample(Calculos.builder().facIntere((new DecimalFormat("#.00")).format(interes))
																							   .fechaIni((new Date()).getTime() + "")
																							   .fechaFinal((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecPag()).getTime() + "")
																							   .moneda(arrCesiones.get(intRegist).get(intPosTerDim).getDocMoneda())
																							   .facContra(arrCesiones.get(intRegist).get(intPosTerDim).getDocContra())
																							   .docCantid(arrCesiones.get(intRegist).get(intPosTerDim).getDocCantid())
																							   .build(), "");

												aplicaIVA = "S";
											}
										}
										
										if (interes.compareTo(BigDecimal.ZERO)<0) {
											interes = interes.multiply(new BigDecimal(-1));
										}
										
										if ("S".equals(aplicaIVA)) {
											canIntIVA = canIntIVA.add(interes).add(new BigDecimal(ivaDocumento.get(0).getIva()));
										} else {
											canIntIVA = canIntIVA.add(interes);
										}
										
										BigDecimal facComisi = new BigDecimal(qryContra.get(0).getConFacCom());
										BigDecimal facApeCom = new BigDecimal(qryFParams.get(0).getParApeCom());
										BigDecimal parIVA = new BigDecimal(qrySParams.get(0).getParIVA());
										
										
										BigDecimal facIntCom = (totalDoc.multiply(facComisi.divide(new BigDecimal(100), 2, RoundingMode.HALF_UP))).add(facApeCom);
										BigDecimal facIVACom = facIntCom.multiply(parIVA).setScale(2, RoundingMode.HALF_UP);
										BigDecimal canComisi = facIntCom.add(facIVACom).setScale(2, RoundingMode.HALF_UP);
										
										String conCobInt = arrCesiones.get(intRegist).get(intPosTerDim).getDocCosto().trim();
										
										if ("03".equals(qryContra.get(0).getConTipFac()) || "05".equals(qryContra.get(0).getConTipFac())) {
											if ("P".equals(conCobInt)) {
												totalDoc = totalDoc.subtract(canComisi.add(canIntIVA)).setScale(2, RoundingMode.HALF_UP);
											} else {
												totalDoc = totalDoc.subtract(canIntIVA).setScale(2, RoundingMode.HALF_UP);
											}
										}
										
										if ("04".equals(qryContra.get(0).getConTipFac()) || "08".equals(qryContra.get(0).getConTipFac()))	{
											if ("P".equals(conCobInt)) {
												totalDoc = totalDoc.subtract(canComisi).setScale(2, RoundingMode.HALF_UP);
											}
										}
										
										if ("07".equals(qryContra.get(0).getConTipFac())) {
											if ("P".equals(conCobInt)) {
												totalDoc = totalDoc.subtract(canComisi.add(canIntIVA)).setScale(2, RoundingMode.HALF_UP);
											}
										}
										
										monto = monto.add(totalDoc);

									}

								} else {
									/*Resta al total la cantidad de esta factura.*/
									dblTotal = dblTotal.subtract(new BigDecimal(arrCesiones.get(intRegist).get(intPosTerDim).getDocCantid()));
		
									/*Registra que no hubo fondos suficientes en la línea.*/
									listQryProvees.get(intRegist).setErrDesFac(1);

									/*Se registra el error de esta factura. 
											Error #1: El saldo de la línea no es suficiente para cubrir el factoraje.*/
									listQryProCliFac.get(intRegProCliFac).setErrDesFac(1);
//										
//									/*Actualiza el número de factura (renglón en la consulta qryProCliFac).*/
									intRegProCliFac++;
								}
							}
							
//							String firmIP = "0";
//							String firmPuerto = "0";
							String modulo;
/*Error if*/							if (monto.compareTo(BigDecimal.ZERO) > 0) {
								if("01".equals(moneda.trim())) {
//									firmIP = conFirmas1.get(0).getCfeIpBaEl();
//									firmPuerto = conFirmas1.get(0).getCfePuBaEl();
									modulo = "SP";
								} else {
//									firmIP = conFirmas2.get(0).getCfeIpBaEl();
//									firmPuerto = conFirmas2.get(0).getCfePuBaEl();
									modulo = "SD";
								}
								
								if (cuentaOrdenante.trim().length() == 18 && cueDest.trim().length() == 18) {
									cuentaOrdenante	= cuentaOrdenante.trim();
									cueDest	= cueDest.trim();
									firmaElectronica = "";
								
									Date fechaFirma =  new Date();
									String fechaFirmaElectronica = (new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_2)).format(fechaFirma);	 								
									try	{
										firmaElectronica = firmasRemotoService.crearFirma(cueDest, cuentaOrdenante, fechaFirmaElectronica, modulo, (new DecimalFormat("#.00")).format(monto)).getResultset().getFirmaElectronica();
										log.info(Constantes.STR_DATOS_PARA_FIRMA + cuentaOrdenante + " " + cueDest + " " + (new DecimalFormat(Constantes.FORMAT_CURRENCY)).format(monto) + " " + 
																										(new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_3)).format(fechaFirma) + "," + fechaLog + Constantes.STR_FAFIRMAELECTRONICA);
									} catch (Exception e) {
										log.error(Constantes.STR_OCURRIO_ERROR + docMovimi + ","+ fechaLog, e.getMessage());														
									}									
									
									/*Transacción para guardar la firma electrónica en tabla otorgamiento.*/
									firmasElectronicasDAO.findByExample(FirmasElectronicas.builder().fieFirma(firmaElectronica) 
																									.fieFecha(fechaFirma.getTime() + "")
																									.fieClient(firCliente)
																									.fieProvee(firProveed)
																									.fieContra(firContrat)
																									.fieLinea(firLinea)
																									.fieMonto(monto.toString())
																									.fieCompan(firCompan)
																									.docMovimi(docMovimi)
																									.fieMoneda(docMoneda)
																									.fieCosto(docCosto).build(),  "");
								}

							}
							
							/*Transacción para generar el alta del factoraje y su consecuente otorgamiento.*/
							List<Factorajes> factoraje = new ArrayList<>();
//							String errMsjOto = "";
							
							/*Se registra el número de cesión para mejor referencia en el reporte.*/
							listQryProvees.get(intRegist).setDocMovimi(docMovimi);
							
							try {
/*Error*/								factoraje = factorajesDAO.findByExample(Factorajes.builder().facMovimi(docMovimi)
																							.docProvee(listQryProvees.get(intRegist).getDocComPro())
																							.build(), "");
								
								/*Se registra el estatus de esta cesión, indicando que tanto el alta como el otorgamiento fueron exitosos.*/
								listQryProvees.get(intRegist).setErrAltOto(0);
							} catch (Exception e) {
								/*Se registra el estatus de esta cesión, indicando que tanto el alta como el otorgamiento no fueron extiosos.*/
								listQryProvees.get(intRegist).setErrAltOto(1);
								if (!factoraje.isEmpty()) {
									listQryProvees.get(intRegist).setErrMsjOto(factoraje.get(0).getErrMensaj());
								}
								/*Las facturas descontadas se regresan a su estado original (reversa).*/
								for (int intPosTerDim=0 ; intPosTerDim<arrCesiones.get(intRegist).size() ; intPosTerDim++) {
									/*Transacción para actualizar el estatus de las notas de crédito de esta cesión.*/
									List<Documentos> rsActuFact1 = documentosUpdateDAO.findByExample(Documentos.builder().docProvee(arrCesiones.get(intRegist).get(intPosTerDim).getDocComPro())
																														 .docNumero(arrCesiones.get(intRegist).get(intPosTerDim).getDocNumero())
																														 .docMovimi(docMovimi)
																														 .docCantid("0")
																														 .docFirmas("0")
																														 .docFecha((new Date()).getTime() + "")
																														 .docFecVen((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecVen()).getTime() + "")
																														 .docFecPag((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(arrCesiones.get(intRegist).get(intPosTerDim).getDocFecPag()).getTime() + "")
																														 .tipActual("L").build(), "");
								}
								
							}
////////////////////////////////intPosSegDim//////////////////////////////////////
						} else {/*Hubo error debido a las firmas de las facturas para este proveedor.*/
							/*Pone el estatus de cada factura con error en el apartado de firmas.*/
							for (int intPosTerDim=0 ; intPosTerDim<arrCesiones.get(intRegist).size() ; intPosTerDim++) {
								if (listQryProvees.get(intRegist).getErrFirmas() == 1) {
									/*Se registra el error de esta factura. 
											Error #1: Firmas*/
									listQryProCliFac.get(intRegProCliFac).setErrFirmas(1);
										
										/*Actualiza el número de factura (renglón en la consulta qryProCliFac).*/
									intRegProCliFac++;
								} else if (listQryProvees.get(intRegist).getErrFirmas() == 2) {
									/*Se registra el error de esta factura. 
											Error #2: Firmas*/
									listQryProCliFac.get(intRegProCliFac).setErrFirmas(2);
										
									/*Actualiza el número de factura (renglón en la consulta qryProCliFac).*/
									intRegProCliFac++;
								} else {
									/*Se registra el error de esta factura. 
											Error #3: Firmas*/
									listQryProCliFac.get(intRegProCliFac).setErrFirmas(3);
										
									/*Actualiza el número de factura (renglón en la consulta qryProCliFac).*/
									intRegProCliFac++;
								}
							}
						}
					}
					
					/*En el caso de que existan errores al momento el alta/otorgamiento de una cesión, se actualiza
					el estatus de cada factura involucrada en dicho movimiento de manera separada, siendo que en 
					un principio sólo se hizo una actualización general del estatus del factoraje.*/
					
/*Error*/					List<DatosProveedores> listQryErrAltOto = listQryProvees.stream().filter(item -> 
													item.getErrAltOto() != null && item.getErrAltOto() == 1).collect(Collectors.toList());

					List<DatosProveedores> listQryAltOtoOK = listQryProvees.stream().filter(item -> 
													item.getErrAltOto() != null && item.getErrAltOto() == 0).collect(Collectors.toList());
					
					if (!listQryErrAltOto.isEmpty()) {
						for (DatosProveedores qryErrAltOto: listQryErrAltOto) {
							for (Documentos qryProCliFac: listQryProCliFac) {
								qryProCliFac.setErrAltOto(qryErrAltOto.getErrAltOto());
								qryProCliFac.setErrMsjOto(qryErrAltOto.getErrMsjOto());
							}
						}
						
						for (Documentos qryProCliFac: listQryProCliFac) {
							if (qryProCliFac.getErrAltOto() == null) {
								qryProCliFac.setErrAltOto(0);
							}
							
							
						}
					} else {
						for (Documentos qryProCliFac: listQryProCliFac) {
							qryProCliFac.setErrAltOto(0);
						}
					}
					
					/*Se agrega el número de cesión a todas las facturas que hayan sido otorgadas correctamente.*/
					if (!listQryAltOtoOK.isEmpty()) {
						for (DatosProveedores qryAltOtoOK: listQryAltOtoOK) {
							/*Transacción para consultar el número de cesión.*/
							List<Documentos> listRsNumCesio = documentosDAO.findByExample(Documentos.builder().docFecIni((new Date()).getTime() + "")
																											  .docFecFin((new Date()).getTime() + "")
																											  .docProvee(qryAltOtoOK.getDocComPro())
																											  .build(), "CL");
							for (Documentos qryProCliFac: listQryProCliFac) {
								for (Documentos rsNumCesio: listRsNumCesio) {
									if (qryAltOtoOK.getDocComPro().equals(qryProCliFac.getDocComPro()) &&
										rsNumCesio.getDocFolio().equals(qryProCliFac.getDocFolio())) {
										qryProCliFac.setDocMovimi(rsNumCesio.getDocFactor());
									}
								}
							}
						}
					}
					
					/*Se genera el reporte que será enviado por correo a las personas con nivel 3.*/
					String docFolio = "";
					String docCantid = "";
					String docMoneda = "";
					String cueClient = "";
					proEmail = "";
					cliEmail = "";
					String tipMoneda = "";
					
					/*Se agregan los registros de la primer consulta al reporte.*/
					for (Documentos qryProCliFac: listQryProCliFac) {
						if("01".equals(qryProCliFac.getDocMoneda())) {
							tipMoneda = Constantes.STR_PESOS;
							
						} else if ("02".equals(qryProCliFac.getDocMoneda())) {
							tipMoneda = Constantes.STR_DOLARES;
						}
						
						if ((qryProCliFac.getErrFirmas() != null && qryProCliFac.getErrFirmas() == 0) &&
							(qryProCliFac.getErrDesFac() != null && qryProCliFac.getErrDesFac() == 0) &&
							(qryProCliFac.getErrAltOto() != null && qryProCliFac.getErrAltOto() == 0)) {
							
							/*Agrega el registro al reporte.*/
							listArrReporteOK.add(ReportesOtorgamiento.builder().docComPro(qryProCliFac.getDocComPro())
																			   .proComple(qryProCliFac.getProComple())
																			   .cliNumero(qryProCliFac.getCliNumero())
																			   .cliComOrd(qryProCliFac.getCliComOrd())
																			   .docFolio(qryProCliFac.getDocFolio())
																			   .docMovimi(qryProCliFac.getDocMovimi())
																			   .docCantid(qryProCliFac.getDocCantid())
																			   .tipMoneda(tipMoneda)
																			   .docFecIni(qryProCliFac.getDocFecIni())
																			   .docFecVen(qryProCliFac.getDocFecVen())
																			   .mensaje("Otorgamiento exitoso.")
																			   .docCosto(qryProCliFac.getDocCosto())
																			   .docNumero(qryProCliFac.getDocNumero())
																			   .conTipLin(qryProCliFac.getConTipLin())
																			   .build());
						} else {
							if (qryProCliFac.getErrFirmas() != null && qryProCliFac.getErrFirmas() != 0){
								switch (qryProCliFac.getErrFirmas()) {
									case 1:
										/*Agrega el registro al reporte.*/
										listArrReportErr.add(ReportesOtorgamiento.builder().docComPro(qryProCliFac.getDocComPro())
												   										   .proComple(qryProCliFac.getProComple())
												   										   .cliNumero(qryProCliFac.getCliNumero())
																						   .cliComOrd(qryProCliFac.getCliComOrd())
																						   .docFolio(qryProCliFac.getDocFolio())
																						   .docMovimi("")
																						   .docCantid(qryProCliFac.getDocCantid())
																						   .tipMoneda(tipMoneda)
																						   .docFecIni(qryProCliFac.getDocFecIni())
																						   .docFecVen(qryProCliFac.getDocFecVen())
																						   .mensaje("El número de Firmas de cada documento debe ser igual.")
																						   .build());
									break;
									case 2:
										/*Agrega el registro al reporte.*/
										listArrReportErr.add(ReportesOtorgamiento.builder().docComPro(qryProCliFac.getDocComPro())
												   										   .proComple(qryProCliFac.getProComple())
												   										   .cliNumero(qryProCliFac.getCliNumero())
																						   .cliComOrd(qryProCliFac.getCliComOrd())
																						   .docFolio(qryProCliFac.getDocFolio())
																						   .docMovimi("")
																						   .docCantid(qryProCliFac.getDocCantid())
																						   .tipMoneda(tipMoneda)
																						   .docFecIni(qryProCliFac.getDocFecIni())
																						   .docFecVen(qryProCliFac.getDocFecVen())
																						   .mensaje("El documento que intenta Descontar ya cuenta con una Firma de este Usuario.")
																						   .build());
									break;
									case 3:
										/*Agrega el registro al reporte.*/
										listArrReportErr.add(ReportesOtorgamiento.builder().docComPro(qryProCliFac.getDocComPro())
												   										   .proComple(qryProCliFac.getProComple())
												   										   .cliNumero(qryProCliFac.getCliNumero())
																						   .cliComOrd(qryProCliFac.getCliComOrd())
																						   .docFolio(qryProCliFac.getDocFolio())
																						   .docMovimi("")
																						   .docCantid(qryProCliFac.getDocCantid())
																						   .tipMoneda(tipMoneda)
																						   .docFecIni(qryProCliFac.getDocFecIni())
																						   .docFecVen(qryProCliFac.getDocFecVen())
																						   .mensaje("El número de Firmas Acumuladas es insuficiente.")
																						   .build());
									break;
									default:
										/*Agrega el registro al reporte.*/
										listArrReportErr.add(ReportesOtorgamiento.builder().docComPro(qryProCliFac.getDocComPro())
												   										   .proComple(qryProCliFac.getProComple())
												   										   .cliNumero(qryProCliFac.getCliNumero())
																						   .cliComOrd(qryProCliFac.getCliComOrd())
																						   .docFolio(qryProCliFac.getDocFolio())
																						   .docMovimi("")
																						   .docCantid(qryProCliFac.getDocCantid())
																						   .tipMoneda(tipMoneda)
																						   .docFecIni(qryProCliFac.getDocFecIni())
																						   .docFecVen(qryProCliFac.getDocFecVen())
																						   .mensaje("Error desconocido de firmas.")
																						   .build());
									break;
								}
							} else if (qryProCliFac.getErrDesFac() != null && qryProCliFac.getErrDesFac() != 0) {
								/*Agrega el registro al reporte.*/
								listArrReportErr.add(ReportesOtorgamiento.builder().docComPro(qryProCliFac.getDocComPro())
										   										   .proComple(qryProCliFac.getProComple())
										   										   .cliNumero(qryProCliFac.getCliNumero())
																				   .cliComOrd(qryProCliFac.getCliComOrd())
																				   .docFolio(qryProCliFac.getDocFolio())
																				   .docMovimi("")
																				   .docCantid(qryProCliFac.getDocCantid())
																				   .tipMoneda(tipMoneda)
																				   .docFecIni(qryProCliFac.getDocFecIni())
																				   .docFecVen(qryProCliFac.getDocFecVen())
																				   .mensaje("Error en el descuento de las facturas.")
																				   .build());
							} else {
								/*Agrega el registro al reporte.*/
								listArrReportErr.add(ReportesOtorgamiento.builder().docComPro(qryProCliFac.getDocComPro())
										   										   .proComple(qryProCliFac.getProComple())
										   										   .cliNumero(qryProCliFac.getCliNumero())
																				   .cliComOrd(qryProCliFac.getCliComOrd())
																				   .docFolio(qryProCliFac.getDocFolio())
																				   .docMovimi("")
																				   .docCantid(qryProCliFac.getDocCantid())
																				   .tipMoneda(tipMoneda)
																				   .docFecIni(qryProCliFac.getDocFecIni())
																				   .docFecVen(qryProCliFac.getDocFecVen())
																				   .mensaje(qryProCliFac.getErrMsjOto())
																				   .build());
							}		
						}
					}
				} else {
					/*No hubo movimientos, se envia mensaje correspondiente.*/
					CorreosDto correo = new CorreosDto();
					correo.setBody("No existen facturas a descontar por el momento.");
					correo.setFrom(notDe);
					correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
					correo.setHtml(true);
					correo.setSubject(notTitulo);
					correo.getTo().add(CORREO_PRUEBA); // Quitar
//					correo.getTo().add("alertaefactor@banregio.com"); // Descomentar
					correo.setCc(notParaOK);
			    	correoRemotoService.enviarCorreo(correo);	
				}
			}
			
			/*Envío de reporte por correo electrónico*/
			if (!listArrReporteOK.isEmpty() || !listArrReportErr.isEmpty()) {
				/*Hubo facturas correctas e incorrectas, se envía correo con reporte incluido.*/
				String bodyCorreo = Constantes.CORREO_OTORGAMIENTO_1;
				if (!listArrReporteOK.isEmpty()) {
					bodyCorreo+=Constantes.CORREO_OTORGAMIENTO_OK_1;
					for (int intArrMov=0 ; intArrMov<listArrReporteOK.size() ; intArrMov++) {
						String fecIni = (new SimpleDateFormat(Constantes.FORMAT_DATE_4, new Locale("es", "ES"))).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(listArrReporteOK.get(intArrMov).getDocFecIni())).replace(".", "");
						String fecVen = (new SimpleDateFormat(Constantes.FORMAT_DATE_4, new Locale("es", "ES"))).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(listArrReporteOK.get(intArrMov).getDocFecVen())).replace(".", "");					
						bodyCorreo+=String.format( Constantes.CORREO_OTORGAMIENTO_OK_2, (intArrMov+1), listArrReporteOK.get(intArrMov).getDocComPro(), listArrReporteOK.get(intArrMov).getProComple(), listArrReporteOK.get(intArrMov).getCliNumero(), listArrReporteOK.get(intArrMov).getCliComOrd(), listArrReporteOK.get(intArrMov).getDocFolio(), listArrReporteOK.get(intArrMov).getDocMovimi(), NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(listArrReporteOK.get(intArrMov).getDocCantid())), listArrReporteOK.get(intArrMov).getTipMoneda(), fecIni, fecVen);
					}
					bodyCorreo+=Constantes.CORREO_OTORGAMIENTO_2;
				}
				
				if (!listArrReportErr.isEmpty()) {
					bodyCorreo+=Constantes.CORREO_OTORGAMIENTO_ERR_1;
					for (int intArrMov=0 ; intArrMov<listArrReportErr.size() ; intArrMov++) {
				
						String fecIni = (new SimpleDateFormat(Constantes.FORMAT_DATE_4, new Locale("es", "ES"))).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(listArrReportErr.get(intArrMov).getDocFecIni())).replace(".", "");
						String fecVen = (new SimpleDateFormat(Constantes.FORMAT_DATE_4, new Locale("es", "ES"))).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(listArrReportErr.get(intArrMov).getDocFecVen())).replace(".", "");
						bodyCorreo+= String.format(Constantes.CORREO_OTORGAMIENTO_ERR_2, (intArrMov+1), listArrReportErr.get(intArrMov).getDocComPro(), listArrReportErr.get(intArrMov).getProComple(), listArrReportErr.get(intArrMov).getCliNumero(), listArrReportErr.get(intArrMov).getCliComOrd(), listArrReportErr.get(intArrMov).getDocFolio(), NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(listArrReportErr.get(intArrMov).getDocCantid())), listArrReportErr.get(intArrMov).getTipMoneda(), fecIni, fecVen, listArrReportErr.get(intArrMov).getMensaje());
					}
					bodyCorreo+=Constantes.CORREO_OTORGAMIENTO_2;
				}

				CorreosDto correo = new CorreosDto();
				correo.setBody(bodyCorreo);
				correo.setFrom(notDe);
				correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
				correo.setHtml(true);
				correo.setSubject(notTitulo);
				correo.getTo().add(CORREO_PRUEBA); // Quitar
//				correo.setTo(notParaOK); // Descomentar
		    	correoRemotoService.enviarCorreo(correo);

			}
			
			/*Se envía correo con los proveedores que excedieron el límite de factoraje.*/
			if (!mensaje.isEmpty()) {
				CorreosDto correo = new CorreosDto();
				correo.setBody(mensaje);
				correo.setFrom(notDe);
				correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
				correo.setHtml(true);
				correo.setSubject("Excedio limite de factoraje");
				correo.getTo().add(CORREO_PRUEBA); // Quitar
//				correo.setTo(notParaErr); // Descomentar
		    	correoRemotoService.enviarCorreo(correo);
			}
			
			if (flag002 == 1) {
				CorreosDto correo = new CorreosDto();
				correo.setBody(errFactura);
				correo.setFrom(notDe);
				correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
				correo.setHtml(true);
				correo.setSubject("Facturas Vencidas");
				correo.getTo().add(CORREO_PRUEBA); // Quitar
//				correo.setTo(notParaErr); // Descomentar
		    	correoRemotoService.enviarCorreo(correo);
			}
			
			if (!menPais.isEmpty()) {
				CorreosDto correo = new CorreosDto();
				correo.setBody(menPais);
				correo.setFrom(notDe);
				correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
				correo.setHtml(true);
				correo.setSubject("Falta Pais Proveedor");
				correo.getTo().add(CORREO_PRUEBA); // Quitar
//				correo.setTo(notParaErr); // Descomentar
		    	correoRemotoService.enviarCorreo(correo);
			}
			
			if (!listArrReporteOK.isEmpty()) {
				List<List<ReportesOtorgamiento>> listDocumentosCliente = new ArrayList<>();
				List<ReportesOtorgamiento> documentos = new ArrayList<>();
				List<List<ReportesOtorgamiento>> listDocumentosProveedor = new ArrayList<>();
				List<ReportesOtorgamiento> listContactosClientes = new ArrayList<>();
				List<String> contactosProveedor = new ArrayList<>();
				
				for (ReportesOtorgamiento arrReporteOK: listArrReporteOK) {
					int encontradocli = 0;
					int encontradopro = 0;
					
					List<Documentos> qryDocume = documentosDAO.findByExample(Documentos.builder().docProvee(arrReporteOK.getDocComPro()).docNumero(arrReporteOK.getDocNumero()).build(), "CP");
					if (qryDocume != null && !qryDocume.isEmpty()) {
						arrReporteOK.setDocCompan(qryDocume.get(0).getDocCompan());
					}
					
					for (List<ReportesOtorgamiento> documentosCliente: listDocumentosCliente) {
						if (arrReporteOK.getDocMovimi() != null && arrReporteOK.getDocMovimi().equals(documentosCliente.get(0).getDocMovimi())) {
							documentosCliente.add(arrReporteOK);
							encontradocli = 1;
						}						
					}
					
					if (encontradocli == 0) {
						documentos = new ArrayList<>();
						documentos.add(arrReporteOK);
						listDocumentosCliente.add(documentos);
					}
					
					for (List<ReportesOtorgamiento> documentosProveedor: listDocumentosProveedor) {
						if (arrReporteOK.getDocComPro().equals(documentosProveedor.get(0).getDocComPro())) {
							documentosProveedor.add(arrReporteOK);
							encontradopro = 1;
						}
					}
					
					if (encontradopro == 0) {
						documentos = new ArrayList<>();
						documentos.add(arrReporteOK);
						listDocumentosProveedor.add(documentos);
					}
						
					String numProveedor = arrReporteOK.getDocComPro();
					String numCliente = arrReporteOK.getCliNumero();
						
					/*Correo de Proveedor*/
					List<Usuarios> listEmailProveedor = usuariosDAO.findByExample(Usuarios.builder().usfNumero(numProveedor).build(), "C3");
					
					int count = 1;
					
					for(Usuarios emailProveedor: listEmailProveedor) {
						if ("A".equals(emailProveedor.getUsfStatus())) {
							if (count == 1) {
								proEmail = emailProveedor.getUsfEmail();
								count = 2;
							} else {
								proEmail+= ", " + emailProveedor.getUsfEmail();
							}
							/*contactos de Proveedor*/
							List<Correos> contactos = correosDAO.findByExample(Correos.builder().parUsuari(emailProveedor.getUsfNumero()).build(), "L1");
							
							for(Correos contacto: contactos) {
								proEmail+=", " + contacto.getFcuEmail();
							}
						}
					}
					listContactosProveedores = new ArrayList<>();
					listContactosProveedores.add(ReportesOtorgamiento.builder().numProveeClie(numProveedor).email(proEmail).build());					
					
					/*Correo de Clientes*/
					List<Usuarios> listEmailCliente = usuariosDAO.findByExample(Usuarios.builder().usfClient(numCliente).build(), "C2");
					count = 1;
					
					for (Usuarios emailCliente: listEmailCliente) {
						if ("A".equals(emailCliente.getUsfStatus())) {
							if (count == 1) {
								cliEmail = emailCliente.getUsfEmail();
								count = 2;
							} else {
								cliEmail+= ", " + emailCliente.getUsfEmail();
							}
							
							/*contactos de cliente*/
							List<Correos> contactos = correosDAO.findByExample(Correos.builder().parUsuari(emailCliente.getUsfNumero()).build(), "L1");
							
							for(Correos contacto: contactos) {
								cliEmail+=", " + contacto.getFcuEmail();
							}
						}
					}
					
					listContactosClientes = new ArrayList<>();
					listContactosClientes.add(ReportesOtorgamiento.builder().numProveeClie(numCliente).email(cliEmail).build());

				}
				
				for (List<ReportesOtorgamiento> documentosCliente: listDocumentosCliente) {
					List<String> contactosCliente = new ArrayList<>();
					
					for (ReportesOtorgamiento contactosClientes: listContactosClientes) {
						if (contactosClientes.getNumProveeClie().equals(documentosCliente.get(0).getCliNumero())) {
							contactosCliente.add(contactosClientes.getEmail());
							break;
						}
					}
					
					List<ReportesOtorgamiento> proveedorArr = new ArrayList<>();
					List<List<ReportesOtorgamiento>> notificacion = new ArrayList<>();
					
					notificacion.add(documentosCliente);
					proveedorArr.add(documentosCliente.get(0));
					notificacion.add(proveedorArr);					
					
					enviaComprobanteDescuento("1", notificacion, notParaNeg, null, contactosCliente,  "1", hoy);
					
					String conTipLin = documentosCliente.get(0).getConTipLin();
					String docMovimi = documentosCliente.get(0).getDocMovimi();
					String docMoneda = documentosCliente.get(0).getTipMoneda();
					String docCantid = documentosCliente.get(0).getDocCantid();
					String docFolio	= documentosCliente.get(0).getDocFolio();
					String numProveedor	= documentosCliente.get(0).getDocComPro();
					String nomProveedor	= documentosCliente.get(0).getProComple();
					String numCliente = documentosCliente.get(0).getCliNumero();
					String nomCliente = documentosCliente.get(0).getCliComOrd();
					String docCosto	= documentosCliente.get(0).getDocCosto();
					String tieneGarantia = "N";
					String cuerpoProveedor = "";
					String cueClient = "";
					String cueProvee = "";
					contactosProveedor = new ArrayList<>();
					
					for(ReportesOtorgamiento contactosProveedores: listContactosProveedores) {
						if (contactosProveedores.getNumProveeClie().equals(numProveedor)) {
							contactosProveedor.add(contactosProveedores.getEmail());
							break;
						}
					}
					
					if (Arrays.asList("2A", "3T", "5C", "7A", "A2", "E1").contains(conTipLin)) {
						/*Transacción para consultar la información general de la cesión.*/
						List<CalculoCesiones> rsInfGen = calculoCesionesDAO.findByExample(CalculoCesiones.builder().facNumero(docMovimi).tipo("I").build(), "");
						
						/*Transacción para consultar los movimientos de la cesión.*/
						List<CalculoCesiones> rsMovCes = calculoCesionesDAO.findByExample(CalculoCesiones.builder().facNumero(docMovimi).tipo("M").build(), "");
						
						/*Consulta las garantias del contrato.*/
						List<Garantias> listResultadosGarantias = garantiasDAO.findByExample(Garantias.builder().parContra(rsInfGen.get(0).getFacContra()).build(), "C1");
						
						for (Garantias resultadosGarantias: listResultadosGarantias) {
							if (resultadosGarantias.getGacGarant().equals("2") &&
								"N".equals(tieneGarantia)) {
								tieneGarantia = "S";
							}
						}
						
						if ("S".equals(tieneGarantia)) {
							enviaCalculoCesion(rsMovCes, rsInfGen.get(0), notParaNeg, listaCorreoAgro, hoy, "1");
						}
						
						if ("E".equals(docCosto.trim())) {
							enviaCalculoCesion(rsMovCes, rsInfGen.get(0), notParaNeg + "," + notParaOK, contactosCliente, hoy, "2");
						} else {
							enviaCalculoCesion(rsMovCes, rsInfGen.get(0), notParaNeg + "," + notParaOK, contactosProveedor, hoy, "3");
						}
	
					} else {
						cueProvee+=String.format(Constantes.CORREO_CUERPO_PROVEEDOR_1, docFolio, (new SimpleDateFormat(Constantes.FORMAT_DATE_2)).format(hoy), NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(docCantid)), docMoneda);
						cueClient = String.format(Constantes.CORREO_CUERPO_CLIENTE, numProveedor, nomProveedor);
						cueClient+=cueProvee + Constantes.CORREO_CUERPO_CLIENTE_PROVEEDOR;
						cuerpoProveedor = String.format(Constantes.CORREO_CUERPO_PROVEEDOR_2, numCliente, nomCliente);
						cuerpoProveedor+= cueProvee + Constantes.CORREO_CUERPO_CLIENTE_PROVEEDOR;
					
						/*Envio de Correo para Cliente y Proveedor*/
						CorreosDto correo = new CorreosDto();
						correo.setBody(cueClient);
						correo.setFrom(notDe);
						correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
						correo.setHtml(true);
						correo.setSubject(Constantes.CORREO_OTORGAMIENTO_SUBJECT);
						correo.getTo().add(CORREO_PRUEBA); // Quitar
//						correo.setTo(new ArrayList<>(Arrays.asList(contactosCliente.split(",")))); // Descomentar
						correo.setCc(new ArrayList<>(Arrays.asList(notParaNeg.split(","))));
				    	correoRemotoService.enviarCorreo(correo);
				    	
				    	correo = new CorreosDto();
						correo.setBody(cuerpoProveedor);
						correo.setFrom(notDe);
						correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
						correo.setHtml(true);
						correo.setSubject(Constantes.CORREO_OTORGAMIENTO_SUBJECT);
						correo.getTo().add(CORREO_PRUEBA); // Quitar
//						correo.setTo(new ArrayList<>(Arrays.asList(contactosProveedor.split(",")))); // Descomentar
						correo.setCc(new ArrayList<>(Arrays.asList(notParaNeg.split(","))));
				    	correoRemotoService.enviarCorreo(correo);
					}
				}
				
				for (int idPro=0 ; idPro< contactosProveedor.size() ; idPro++) {
					List<String> contactosProveedorEmail = new ArrayList<>();
					for (ReportesOtorgamiento contactosProveedores: listContactosProveedores) {
						if (contactosProveedores.getNumProveeClie().equals(listDocumentosProveedor.get(idPro).get(0).getNumProveeClie())) {
							contactosProveedorEmail.add(contactosProveedores.getEmail());
							break;
						}
					}
					
					List<ReportesOtorgamiento> proveedorArr = new ArrayList<>();
					List<List<ReportesOtorgamiento>> notificacion = new ArrayList<>();
					ReportesOtorgamiento reportesOtorgamiento = new ReportesOtorgamiento();
					reportesOtorgamiento.setNumProveeClie(listDocumentosProveedor.get(idPro).get(0).getNumProveeClie());
					reportesOtorgamiento.setEmail(listDocumentosProveedor.get(idPro).get(0).getEmail());
					
					notificacion.add(listDocumentosProveedor.get(idPro));
					proveedorArr.add(reportesOtorgamiento);				
					notificacion.add(proveedorArr);
					
					enviaComprobanteDescuento("2", notificacion, notParaNeg, contactosProveedor.stream().collect(Collectors.joining(",")), new ArrayList<>(), "1", hoy);
				}
			}
			
			respuesta.setStatusCode("000000");
			respuesta.setStatusMessage("El proceso se ejecuto exitosamente.");
			return respuesta;
		} catch (Exception e) {
			e.printStackTrace();
			CorreosDto correo = new CorreosDto();
			correo.setBody("Se detectó el siguiente error en la aplicación: " + e.getMessage());
			correo.setFrom(notDe);
			correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
			correo.setHtml(true);
			correo.setSubject("Error en descuento/otorgamiento automático - eFactor");
			correo.getTo().add(CORREO_PRUEBA); // Quitar
//			correo.getTo().add("alertaefactor@banregio.com"); // Descomentar
			correo.getCc().add(CORREO_PRUEBA);
//			correo.setCc(notParaErr);
	    	correoRemotoService.enviarCorreo(correo);
	    	
	    	respuesta.setStatusCode("000001");
			respuesta.setStatusMessage("Ocurrio un error al ejecutar el proceso: " + e.getMessage());
			return respuesta;
		}
	}
	
	
	@SuppressWarnings("unchecked")
//	@SafeVarargs
	private static List<Documentos> ordenarLista(List<Documentos> resultados, Comparator<Documentos>... comparators) {
	    Comparator<Documentos> combinedComparator = Comparator
	            .nullsLast((d1, d2) -> 0);

	    for (Comparator<Documentos> comparator : comparators) {
	        combinedComparator = combinedComparator.thenComparing(comparator);
	    }

	    return resultados.stream()
	            .sorted(combinedComparator)
	            .collect(Collectors.toList());
	}
	
	public void enviaComprobanteDescuento(String pantalla, List<List<ReportesOtorgamiento>> listNotificacion, String listaCorreoNegocio, String listaCorreoProveedor, List<String> listaCorreoCliente, String firma, Date fechahora) throws Exception {
		
		List<String> listacorreo = new ArrayList<>();
		if (!listaCorreoCliente.isEmpty()) {
			listacorreo = new ArrayList<>(listaCorreoCliente);
		}
		if (listaCorreoProveedor != null && !listaCorreoProveedor.isEmpty()){
			listacorreo.addAll(new ArrayList<>(Arrays.asList(listaCorreoProveedor.split(","))));
		}
		if (listaCorreoNegocio != null && !listaCorreoNegocio.isEmpty()) {
			listacorreo.addAll(new ArrayList<>(Arrays.asList(listaCorreoNegocio.split(","))));	
		}
		
		
		String numero = listNotificacion.get(1).get(0).getNumProveeClie();
		String nombre = listNotificacion.get(1).get(0).getEmail();
		String titulo = "";
		String asunto = "";
		String usuario = "";
		
		if ("1".equals(pantalla)) {
			titulo = "Notificación de Transmisión Derechos de Crédito <br/>Descuento Automático";
			asunto = "Notificación de Transmisión de Derechos";
			usuario = "C";
		}
		if ("2".equals(pantalla)) {
			titulo = "Aviso de documentos transmitidos en modalidad Descuento Automático";
			asunto = "Aviso de Descuento Automático de Documentos";
			usuario = "P";
		}
		
		//Escritura en archivo log
		
		List<ReportesOtorgamiento> listDocsNotificacion = new ArrayList<>();
		List<String> contratos = new ArrayList<>();
		
		if (listNotificacion!=null && listNotificacion.get(0)!=null && !listNotificacion.get(0).isEmpty()) {
			for (ReportesOtorgamiento notificacion: listNotificacion.get(0)) {
				String fechaPubicacion;
				String tipoCosto;
				String compan;
				if (!notificacion.getDocFecIni().isEmpty()) {
					/*Consultas de bitacora para optener fecha de publiación.*/
					List<BitacoraMovimientos> result = bitacoraMovimientosDAO.findByExample(BitacoraMovimientos.builder().cliente(notificacion.getCliNumero())
																										   				 .proveedor(numero)
																										   				 .tipOperac("001")
																										   				 .fechaIni("")
																										   				 .fechaFin("")
																										   				 .tipDocume("")
																										   				 .build(), "CK");
					
					if(result!=null && !result.isEmpty()) {
						fechaPubicacion = result.get(0).getBitFecha();
					} else {
						fechaPubicacion = "";
					}
				} else {
					fechaPubicacion = "";
				}
				
				if ("E".equals(notificacion.getDocCosto().trim())) {
					tipoCosto = "Emisor";
				} else {
					tipoCosto = "Proveedor";
				}
				
				if ("500".equals(notificacion.getConTipLin().trim())) {
					compan = "START";
				} else {
					compan = "BR";
				}
				
				ReportesOtorgamiento doc = new ReportesOtorgamiento();
				doc.setDocFolio(notificacion.getDocFolio());
				doc.setDocCantid(notificacion.getDocCantid());
				doc.setTipMoneda(notificacion.getTipMoneda());
				if (!notificacion.getCliComOrd().isEmpty()){
					doc.setDocFecIni(notificacion.getDocFecIni());
					doc.setDocFecVen(notificacion.getDocFecVen());
					doc.setFecPublic(fechaPubicacion);
				} else {
					doc.setDocFecIni("");
					doc.setDocFecVen("");
					doc.setFecPublic("");
				}
				doc.setTipoCosto(tipoCosto);
				doc.setCompan(compan);
				doc.setCliNumero(notificacion.getCliNumero());
				doc.setCliComOrd(notificacion.getCliComOrd());
				listDocsNotificacion.add(doc);
			}
			
			String numeroCliente = "";
			
			String bodyCorreo = String.format(Constantes.CORREO_CUERPO_COMPORBANTE_DESC_1, logoStart64, logoStart64, logoEfactor64, titulo);
			if ("1".equals(pantalla)) {
				bodyCorreo+=Constantes.CORREO_CUERPO_COMPORBANTE_DESC_2;
			} else if ("2".equals(pantalla)) {
				bodyCorreo+=Constantes.CORREO_CUERPO_COMPORBANTE_DESC_3;
			}
			bodyCorreo+=String.format(Constantes.CORREO_CUERPO_COMPORBANTE_DESC_4, numero, nombre);
			
			BigDecimal stPesos = BigDecimal.ZERO;
			BigDecimal stDolares = BigDecimal.ZERO;
			
			for(ReportesOtorgamiento docsNotificacion: listDocsNotificacion) {
				if (!numeroCliente.equals(docsNotificacion.getCliNumero()) && 
					!docsNotificacion.getCliNumero().isEmpty()) {
					if (!numeroCliente.isEmpty()) {
						bodyCorreo+=String.format(Constantes.CORREO_CUERPO_COMPORBANTE_DESC_5, NumberFormat.getCurrencyInstance(Locale.US).format(stPesos), NumberFormat.getCurrencyInstance(Locale.US).format(stDolares));
						stPesos = BigDecimal.ZERO;
						stDolares = BigDecimal.ZERO;
					}
					bodyCorreo+=String.format(Constantes.CORREO_CUERPO_COMPORBANTE_DESC_6, docsNotificacion.getCliNumero(), docsNotificacion.getCliComOrd());
				}
				numeroCliente = docsNotificacion.getCliNumero();
				
				if ("C".equals(usuario)) {
					numero = docsNotificacion.getCliNumero();
					nombre = docsNotificacion.getCliComOrd();
				}
				
				if (Constantes.STR_PESOS.equals(docsNotificacion.getTipMoneda())) {
					stPesos = stPesos.add(new BigDecimal(docsNotificacion.getDocCantid()));
				} else {
					stDolares = stDolares.add(new BigDecimal(docsNotificacion.getDocCantid()));
				}
				bodyCorreo+=String.format(Constantes.CORREO_CUERPO_COMPORBANTE_DESC_7, docsNotificacion.getDocFolio(), NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(docsNotificacion.getDocCantid())), docsNotificacion.getTipMoneda(), docsNotificacion.getDocFecIni(), docsNotificacion.getDocFecVen(), docsNotificacion.getFecPublic(), docsNotificacion.getTipoCosto(), docsNotificacion.getCompan());
			}
			bodyCorreo+=String.format(Constantes.CORREO_CUERPO_COMPORBANTE_DESC_8, NumberFormat.getCurrencyInstance(Locale.US).format(stPesos), NumberFormat.getCurrencyInstance(Locale.US).format(stDolares));			
			stPesos = BigDecimal.ZERO;
			stDolares = BigDecimal.ZERO;
			bodyCorreo+=Constantes.CORREO_CUERPO_COMPORBANTE_DESC_9;
			if ("1".equals(firma)) {		
				bodyCorreo+=String.format(Constantes.CORREO_CUERPO_COMPORBANTE_DESC_10, numero, nombre, (new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_4)).format(fechahora));
			}				
			bodyCorreo+=Constantes.CORREO_CUERPO_COMPORBANTE_DESC_11;
			
			String doc = SchedulerAltaFacturacionAutomatica.crearPDF(bodyCorreo);
			String archivopdf = new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_5).format(fechahora) + ".pdf";
			
			CorreosDto correo = new CorreosDto();
			correo.setBody("");
			correo.setFrom(notDe);
			correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
			correo.setHtml(true);
			correo.setSubject(asunto + " " + numero + " " + nombre + " " + new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_4).format(fechahora));
			correo.getTo().add(CORREO_PRUEBA); // Quitar
//			correo.setTo(listacorreo); // Descomentar
			correo.setAdjuntos(Arrays.asList(AdjuntosDto.builder().bytes(doc).fileName(archivopdf).build()));
	    	correoRemotoService.enviarCorreo(correo);
		}
	}
	
	public void enviaCalculoCesion(List<CalculoCesiones> listMovimientos, CalculoCesiones infGeneral, String listaCorreoNegocio, List<String> listaCorreoAgro, Date fechahora, String destino) throws Exception {

		List<String> listacorreo = new ArrayList<>(listaCorreoAgro);
		if (listaCorreoNegocio.isEmpty()) {
			listacorreo.addAll(new ArrayList<>(Arrays.asList(listaCorreoNegocio.split(","))));
		}
		
		String titulo = "";
		String asunto = "";
		String descripcion = "";
		BigDecimal total = BigDecimal.ZERO;
		BigDecimal valNor = BigDecimal.ZERO;
		BigDecimal valAnt = BigDecimal.ZERO;
		BigDecimal intere = BigDecimal.ZERO;
		BigDecimal intereT = BigDecimal.ZERO;
		BigDecimal comisi = BigDecimal.ZERO;
		BigDecimal iVAComi = BigDecimal.ZERO;
		BigDecimal sTotal = BigDecimal.ZERO;
		BigDecimal iVACom = BigDecimal.ZERO;
		String rowcolor = Constantes.STR_COLOR;
		
		if ("1".equals(destino)) {
			titulo = "Cálculo de Cesión Garantía FEGA";
			asunto = "CALCULO DE CESION GARANTIA FEGA";
			descripcion = "Adjunto cálculo de cesión de la operación " + infGeneral.getNumCesion() + " correspondiente al cliente " + infGeneral.getCliComple() + " para su garantización.";
		} else if ("3".equals(destino)) {
			titulo = Constantes.CORREO_OTORGAMIENTO_SUBJECT;
			asunto = "Calculo de Cesion Documentos Transmitidos";
			descripcion = "";
		} else {
			titulo = Constantes.CORREO_OTORGAMIENTO_SUBJECT;
			asunto = Constantes.CORREO_OTORGAMIENTO_SUBJECT;
			descripcion = "";
		}
		
		//Escritura en archivo log
		
		String cuerpo = "";
		String encabezado = "";
		String pie = "";
		
		encabezado+=String.format(Constantes.CORREO_ENCABEZADO_CALCULO_CESION_1, logoStart64, logoEfactor64, titulo, descripcion);
		cuerpo+=Constantes.CORREO_CUERPO_CALCULO_CESION_1;

//		for () {
			String tasaBase = infGeneral.getTasa();
			cuerpo+=String.format(Constantes.CORREO_CUERPO_CALCULO_CESION_2, infGeneral.getCliComple(), NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(infGeneral.getValNomina())), (new DecimalFormat("00.0000")).format(new BigDecimal(tasaBase)), NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(infGeneral.getFacCantid())), infGeneral.getFacTasSob(), (new DecimalFormat(Constantes.FORMAT_CURRENCY_1)).format(new BigDecimal(infGeneral.getFacAforo())), infGeneral.getFacTasa(), (new DecimalFormat(Constantes.FORMAT_CURRENCY_1)).format(new BigDecimal(infGeneral.getValAfoRet())), infGeneral.getFacCuenta().substring(0, 3), infGeneral.getFacCuenta().substring(3, 8), infGeneral.getFacCuenta().substring(8, 11), infGeneral.getFacCuenta().substring(11, 12), (new DecimalFormat(Constantes.FORMAT_CURRENCY_1)).format(new BigDecimal(infGeneral.getFacComisi())), infGeneral.getFacContra(), (new SimpleDateFormat("dd/MM/yyyy")).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(infGeneral.getFacFecIni())), infGeneral.getNumCesion().substring(0, 3), infGeneral.getNumCesion().substring(3, 8), infGeneral.getNumCesion().substring(8, 11), infGeneral.getNumCesion().substring(11, 12), infGeneral.getNumCesion().substring(12, 15));
			if ("01".equals(infGeneral.getFacMoneda())) {
				cuerpo+=Constantes.CORREO_CUERPO_CALCULO_CESION_3;
			} else {
				cuerpo+=Constantes.CORREO_CUERPO_CALCULO_CESION_4;
			}
			cuerpo+=Constantes.CORREO_CUERPO_CALCULO_CESION_5;
//		}
		
		cuerpo+=Constantes.CORREO_CUERPO_CALCULO_CESION_6;
		
		for (CalculoCesiones movimientos: listMovimientos) {
			BigDecimal impTotal = BigDecimal.ZERO;
			BigDecimal comis;
			String strAltLef = null;
			String strAltRig = null;
			
			if (movimientos.getValIVACom().trim().isEmpty()) {
				iVACom = BigDecimal.ZERO;
			} else {
				iVACom = new BigDecimal(movimientos.getValIVACom());
			}
			
			if (movimientos.getValComisi().trim().isEmpty()) {
				comis = BigDecimal.ZERO;
			} else {
				comis = new BigDecimal(movimientos.getValComisi());
			}
			
			if (movimientos.getValIntere().trim().isEmpty()) {
				intere = BigDecimal.ZERO;
			} else {
				intere = new BigDecimal(movimientos.getValIntere());
			}
			impTotal = (new BigDecimal(movimientos.getValAntici())).subtract(intere.subtract(comis.subtract(iVACom)));
			
			if ("1".equals(movimientos.getTidNatura())) {
				strAltLef = "";
				strAltRig = "";
			} else if ("2".equals(movimientos.getTidNatura())) {
				strAltLef = "(<nbsp/><nbsp/>";
				strAltRig = ")";
			}
			
			String fecPag = (new SimpleDateFormat("dd/MM/yyyy")).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(movimientos.getDocFecPag()));
			String fecPlazo = (new SimpleDateFormat("dd/MM/yyyy")).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(movimientos.getFecPlazo()));
			
			cuerpo+=String.format(Constantes.CORREO_CUERPO_CALCULO_CESION_7, movimientos.getDocNumero(), movimientos.getComComple(), movimientos.getDocFolio(), fecPag, fecPlazo, strAltLef, NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(movimientos.getDocCantid())), strAltRig, strAltLef, NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(movimientos.getValAntici())), strAltRig, strAltLef, NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(movimientos.getValIntere())), strAltRig, NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(movimientos.getValComisi())), strAltLef, NumberFormat.getCurrencyInstance(Locale.US).format(new BigDecimal(movimientos.getValIVACom())), strAltRig, strAltLef, NumberFormat.getCurrencyInstance(Locale.US).format(impTotal), strAltRig);
			
			if ("1".equals(movimientos.getTidNatura())) {
				valNor	= valNor.add(new BigDecimal(movimientos.getDocCantid()));
				valAnt	= valAnt.add(new BigDecimal(movimientos.getValAntici()));
				intereT	= intereT.add(new BigDecimal(movimientos.getValIntere()));
				comisi	= comisi.add(new BigDecimal(movimientos.getValComisi()));
			} else if ("2".equals(movimientos.getTidNatura())) {
				valNor	= valNor.subtract(new BigDecimal(movimientos.getDocCantid()));
				valAnt	= valAnt.subtract(new BigDecimal(movimientos.getValAntici()));
				intereT	= intereT.subtract(new BigDecimal(movimientos.getValIntere()));
				comisi	= comisi.subtract(new BigDecimal(movimientos.getValComisi()));
			}
			if (!movimientos.getValIVACom().isEmpty()) {
				if ("1".equals(movimientos.getTidNatura())) {
					iVAComi	= iVAComi.add(new BigDecimal(movimientos.getValIVACom()));
				} else if ("2".equals(movimientos.getTidNatura())) {
					iVAComi	= iVAComi.subtract(new BigDecimal(movimientos.getValIVACom()));
				}
			}
			if ("1".equals(movimientos.getTidNatura())) {
				sTotal	= sTotal.add(impTotal);
			} else if ("2".equals(movimientos.getTidNatura())) {
				sTotal	= sTotal.subtract(impTotal);
			}
			
			rowcolor = rowcolor.equals(Constantes.STR_COLOR) ? "ffffff" : Constantes.STR_COLOR;			
		}
		
		cuerpo+=String.format(Constantes.CORREO_CUERPO_CALCULO_CESION_8, NumberFormat.getCurrencyInstance(Locale.US).format(valNor), NumberFormat.getCurrencyInstance(Locale.US).format(valAnt), NumberFormat.getCurrencyInstance(Locale.US).format(intereT), NumberFormat.getCurrencyInstance(Locale.US).format(comisi), NumberFormat.getCurrencyInstance(Locale.US).format(iVAComi), NumberFormat.getCurrencyInstance(Locale.US).format(sTotal));
		pie+=Constantes.CORREO_PIE_CALCULO_CESION;
		
		String cuerpoReporte = "";
		String moneda = "";
//		for() {
			if ("01".equals(infGeneral.getFacMoneda())) {
				moneda = Constantes.STR_PESOS;
			} else if ("02".equals(infGeneral.getFacMoneda())) {
				moneda = Constantes.STR_DOLARES;
			}
			
			String nombreCliente = infGeneral.getCliComple().replace(",", " ");
			
			tasaBase = (new BigDecimal(infGeneral.getFacTasa())).subtract(new BigDecimal(infGeneral.getFacTasSob())).toString();

			cuerpoReporte+=" , , , , , , , , , ,\n";
			cuerpoReporte+=" , , , ,CALCULO DE CESION, , , , , ,\n";
			cuerpoReporte+=" , , , ," + nombreCliente + ", , , , , ,\n";
			cuerpoReporte+=" , , ,VALOR NOMINAL:," + infGeneral.getValNomina() + ", TASA BASE:," + (new DecimalFormat("99.0000")).format(new BigDecimal(tasaBase)) + " %\n";
			cuerpoReporte+=" , , ,VALOR ANTICIPO:," + infGeneral.getFacCantid() + ", PUNTOS:," + infGeneral.getFacTasSob()+ "\n";
			cuerpoReporte+=" , , ,AFORO DE CESION:," + (new DecimalFormat(Constantes.FORMAT_CURRENCY_2)).format(new BigDecimal(infGeneral.getFacAforo())) + " %, TASA REAL:," + infGeneral.getFacTasa() + " %\n";
			cuerpoReporte+=" , , ,AFORO RETENIDO:," + (new DecimalFormat(Constantes.FORMAT_CURRENCY_2)).format(new BigDecimal(infGeneral.getValAfoRet())) +" %, CUENTA CHEQUES:," + infGeneral.getFacCuenta().substring(0, 3)+ "-" + infGeneral.getFacCuenta().substring(3, 8) + "-" + infGeneral.getFacCuenta().substring(8, 11) + "-" +infGeneral.getFacCuenta().substring(11, 12) + "\n";
			cuerpoReporte+=" , , ,PORCENTAJE COMISION:," + (new DecimalFormat(Constantes.FORMAT_CURRENCY_2)).format(new BigDecimal(infGeneral.getFacComisi())) + " %, NO. CONTRATO:," + infGeneral.getFacContra() + "\n";
			cuerpoReporte+=" , , ,FECHA DE OPERACION:," + (new SimpleDateFormat("dd/MM/yyyy")).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(infGeneral.getFacFecIni())) + ", NUMERO DE CESION:," + infGeneral.getNumCesion().substring(0, 3) + "-" + infGeneral.getNumCesion().substring(3, 8) + "-" + infGeneral.getNumCesion().substring(8, 11) + "-" + infGeneral.getNumCesion().substring(11, 12) + "-" + infGeneral.getNumCesion().substring(12, 15) + "\n";
			cuerpoReporte+=" , , ,MONEDA:," + moneda + ", , \n";
			cuerpoReporte+=" , , , , , , \n";
//		}
			
			cuerpoReporte+=" , , , ,CALCULO DE CESION, , , , , ,\n";
			cuerpoReporte+="OC,PROVEEDOR,REF,FECHA VENCIMIENTO,DIAS,VALOR NOMINAL,VALOR ANTICIPO,INTERESES,COMISION,IVA DE COMISION,IMPORTE TOTAL\n";
			
			total = BigDecimal.ZERO;
			valNor = BigDecimal.ZERO;
			valAnt = BigDecimal.ZERO;
			intere = BigDecimal.ZERO;
			intereT = BigDecimal.ZERO;
			comisi = BigDecimal.ZERO;
			iVAComi = BigDecimal.ZERO;
			iVACom = BigDecimal.ZERO;
			sTotal = BigDecimal.ZERO;
			
			for (CalculoCesiones movimientos: listMovimientos) {
				BigDecimal impTotal = BigDecimal.ZERO;
				BigDecimal comis;
				String strAltLef = null;
				String strAltRig = null;
				
				if (movimientos.getValIVACom().trim().isEmpty()) {
					iVACom = BigDecimal.ZERO;
				} else {
					iVACom =new BigDecimal(movimientos.getValIVACom());
				}
				if (movimientos.getValComisi().trim().isEmpty()) {
					comis = BigDecimal.ZERO;
				} else {
					comis =new BigDecimal(movimientos.getValComisi());
				}
				if (movimientos.getValIntere().trim().isEmpty()) {
					intere = BigDecimal.ZERO;
				} else {
					intere =new BigDecimal(movimientos.getValIntere());
				}
				
				impTotal = (new BigDecimal(movimientos.getValAntici())).subtract(intere.subtract(comis.subtract(iVACom)));

				if ("1".equals(movimientos.getTidNatura())) {
					strAltLef = "";
					strAltRig = "";
				} else if ("2".equals(movimientos.getTidNatura())) {
					strAltLef = "(";
					strAltRig = ")";
				}
				
				if ("1".equals(movimientos.getTidNatura())) {
					valNor	= valNor.add(new BigDecimal(movimientos.getDocCantid()));
					valAnt	= valAnt.add(new BigDecimal(movimientos.getValAntici()));
					intereT	= intereT.add(new BigDecimal(movimientos.getValIntere()));
					comisi	= comisi.add(new BigDecimal(movimientos.getValComisi()));
				} else if ("2".equals(movimientos.getTidNatura())) {
					valNor	= valNor.subtract(new BigDecimal(movimientos.getDocCantid()));
					valAnt	= valAnt.subtract(new BigDecimal(movimientos.getValAntici()));
					intereT	= intereT.subtract(new BigDecimal(movimientos.getValIntere()));
					comisi	= comisi.subtract(new BigDecimal(movimientos.getValComisi()));
				}
				
				if (!movimientos.getValIVACom().isEmpty()) {
					if ("1".equals(movimientos.getTidNatura())) {
						iVAComi	= iVAComi.add(new BigDecimal(movimientos.getValIVACom()));
					} else if ("2".equals(movimientos.getTidNatura())) {
						iVAComi	= iVAComi.subtract(new BigDecimal(movimientos.getValIVACom()));
					}
				}
				if ("1".equals(movimientos.getTidNatura())) {
					sTotal	= sTotal.add(impTotal);
				} else if ("2".equals(movimientos.getTidNatura())) {
					sTotal	= sTotal.subtract(impTotal);
				}
				
				String nombre = infGeneral.getCliComple().replace(",", " ");
				
				if(nombre.length()>20){
					nombre = nombre.substring(0, 20);
				}

				cuerpoReporte+= infGeneral.getDocNumero() + "," + nombre + "," + infGeneral.getDocFolio() + "," + (new SimpleDateFormat("dd/MM/yyyy")).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(infGeneral.getDocFecPag())) + "," + (new SimpleDateFormat("dd/MM/yyyy")).format((new SimpleDateFormat(Constantes.FORMAT_FULL_TIME)).parse(infGeneral.getFecPlazo())) + "," + strAltLef + infGeneral.getDocCantid() + strAltRig + "," + strAltLef + infGeneral.getValAntici() + strAltRig + "," + strAltLef + infGeneral.getValIntere() + strAltRig + "," + infGeneral.getValComisi() + "," + infGeneral.getValIVACom() + "," + strAltLef + impTotal.toString()+ strAltRig + "\n";
				
				cuerpoReporte+=" , , , ,TOTALES:," + valNor + "," + valAnt + "," + intereT + "," + comisi + "," + iVACom + "," + sTotal + "\n";
			}
			
			String docpdf = SchedulerAltaFacturacionAutomatica.crearPDF(cuerpo);
			String archivopdf = new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_5).format(fechahora) + ".pdf";
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(cuerpoReporte.getBytes());
			byte[] bytes = baos.toByteArray();
			String doccsv = Base64.getEncoder().encodeToString(bytes);
			String archivocsv = new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_5).format(fechahora) + ".csv";
			
			CorreosDto correo = new CorreosDto();
			correo.setBody(encabezado+cuerpo+pie);
			correo.setFrom(notDe);
			correo.setFromName(Constantes.CORREO_OTORGAMIENTO_FROM_NAME);
			correo.setHtml(true);
			correo.setSubject(asunto + " " + new SimpleDateFormat(Constantes.FORMAT_FULL_TIME_4).format(fechahora));
			correo.getTo().add(CORREO_PRUEBA); // Quitar
//			correo.setTo(listacorreo); // Descomentar
			correo.setAdjuntos(Arrays.asList(AdjuntosDto.builder().bytes(docpdf).fileName(archivopdf).build(),
											 AdjuntosDto.builder().bytes(doccsv).fileName(archivocsv).build()));
	    	correoRemotoService.enviarCorreo(correo);
			
	}
}
