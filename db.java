import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.math3.stat.Frequency;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.numbers.NumberColumnFormatter;


/*********************************
 *     electrical data db connection
 *********************************/   


/*
 * 
 * 
dbo, TblDie, FldStep, varchar, 8, 0
dbo, TblDie, FldRun, varchar, 8, 0
dbo, TblDie, FldLot, varchar, 16, 0
dbo, TblDie, FldWafer, varchar, 8, 0
dbo, TblDie, FldI, int, 10, 0
dbo, TblDie, FldJ, int, 10, 0
dbo, TblDie, FldFailBin, varchar, 1, 1
dbo, TblDie, FldFailError, varchar, 1, 1
dbo, TblDie, FldFailBinOut, varchar, 64, 1
dbo, TblDie, FldNote, varchar, 128, 1

dbo, TblWafer, FldStep, varchar, 8, 0
dbo, TblWafer, FldRun, varchar, 8, 0
dbo, TblWafer, FldLot, varchar, 16, 0
dbo, TblWafer, FldWafer, varchar, 8, 0
dbo, TblWafer, FldPartType, varchar, 16, 1
dbo, TblWafer, FldStepInDateTime, datetime, 23, 1
dbo, TblWafer, FldStepOutDateTime, datetime, 23, 1
dbo, TblWafer, FldStepState, varchar, 16, 1
dbo, TblWafer, FldMapVersion, varchar, 32, 1
dbo, TblWafer, FldProgram, varchar, 32, 1
dbo, TblWafer, FldProgramRevision, varchar, 16, 1
dbo, TblWafer, FldStation, varchar, 16, 1
dbo, TblWafer, FldProbeCard, varchar, 16, 1
dbo, TblWafer, FldPassDpw, int, 10, 1
dbo, TblWafer, FldNote, varchar, 256, 1

dbo, TblLot, FldFab, varchar, 8, 0
dbo, TblLot, FldStep, varchar, 8, 0
dbo, TblLot, FldLot, varchar, 16, 0
dbo, TblLot, FldDevice, varchar, 8, 1
dbo, TblLot, FldPartType, varchar, 16, 1
dbo, TblLot, FldStartWaferQty, int, 10, 1
dbo, TblLot, FldStepInWaferQty, int, 10, 1
dbo, TblLot, FldStepOutWaferQty, int, 10, 1
dbo, TblLot, FldStepInDateTime, datetime, 23, 1
dbo, TblLot, FldStepOutDateTime, datetime, 23, 1
dbo, TblLot, FldStepWaferQtyState, varchar, 16, 1
dbo, TblLot, FldStepState, varchar, 16, 1
dbo, TblLot, FldYieldMeanDpw, float, 53, 1
dbo, TblLot, FldYieldMedianDpw, float, 53, 1
dbo, TblLot, FldYieldStdDevDpw, float, 53, 1
dbo, TblLot, FldYieldPct10Dpw, float, 53, 1
dbo, TblLot, FldYieldPct25Dpw, float, 53, 1
dbo, TblLot, FldYieldPct75Dpw, float, 53, 1
dbo, TblLot, FldYieldPct90Dpw, float, 53, 1
dbo, TblLot, FldYieldMinDpw, int, 10, 1
dbo, TblLot, FldYieldMaxDpw, int, 10, 1
dbo, TblLot, FldYieldRangeDpw, int, 10, 1
dbo, TblLot, FldNote, varchar, 256, 1


  dbo, TblAnalysis, FldId, int identity, 10, 0
  dbo, TblAnalysis, FldYear, int, 10, 1
  dbo, TblAnalysis, FldMonth, int, 10, 1
  dbo, TblAnalysis, FldWorkWeek, int, 10, 1
  dbo, TblAnalysis, FldState, varchar, 16, 1
  dbo, TblAnalysis, FldProductPhase, varchar, 16, 1
  dbo, TblAnalysis, FldProbeStep, varchar, 8, 1
  dbo, TblAnalysis, FldDevice, varchar, 16, 1
  dbo, TblAnalysis, FldPartType, varchar, 16, 1
  dbo, TblAnalysis, FldLot, varchar, 16, 1
  dbo, TblAnalysis, FldWafer, varchar, 128, 1
  dbo, TblAnalysis, FldDiePass, int, 10, 1
  dbo, TblAnalysis, FldAnalyst, varchar, 16, 1
  dbo, TblAnalysis, FldBin, varchar, 256, 1
  dbo, TblAnalysis, FldKind, varchar, 16, 1
  dbo, TblAnalysis, FldShape, varchar, 16, 1
  dbo, TblAnalysis, FldPosition, varchar, 16, 1
  dbo, TblAnalysis, FldCause, varchar, 32, 1
  dbo, TblAnalysis, FldLevel, varchar, 64, 1
  dbo, TblAnalysis, FldDefect, varchar, 64, 1
  dbo, TblAnalysis, FldEquipment, varchar, 64, 1
  dbo, TblAnalysis, FldArea, varchar, 64, 1
  dbo, TblAnalysis, FldFacility, varchar, 16, 1
  dbo, TblAnalysis, FldNeedPfa, bit, 1, 1
  dbo, TblAnalysis, FldNeedParamTest, bit, 1, 1
  dbo, TblAnalysis, FldWaferBack, bit, 1, 1
  dbo, TblAnalysis, FldAccepted, varchar, 8, 1
  dbo, TblAnalysis, FldNote, varchar, 2147483647, 1
  dbo, TblAnalysis, FldSwr, varchar, 256, 1
  dbo, TblAnalysis, FldQdr, varchar, 256, 1
  dbo, TblAnalysis, FldWfms, varchar, 256, 1
  dbo, TblAnalysis, FldFais, varchar, 256, 1
  dbo, TblAnalysis, FldImporter, varchar, 32, 1
  dbo, TblAnalysis, FldNoteCustomer, varchar, 512, 1
  dbo, TblAnalysis, FldFeedback, varchar, 512, 1
  dbo, TblAnalysis, FldRun, varchar, 8, 1
  dbo, TblAnalysis, FldExcursion, varchar, 32, 1


  dbo, TblWaferRma, FldId, int, 10, 0
  dbo, TblWaferRma, FldState, varchar, 16, 1
  dbo, TblWaferRma, FldYear, int, 10, 1
  dbo, TblWaferRma, FldWorkWeek, int, 10, 1
  dbo, TblWaferRma, FldPartType, varchar, 16, 1
  dbo, TblWaferRma, FldLot, varchar, 16, 1
  dbo, TblWaferRma, FldWafer, varchar, 8, 1
  dbo, TblWaferRma, FldDiePass, int, 10, 1
  dbo, TblWaferRma, FldDefect, varchar, 128, 1
  dbo, TblWaferRma, FldWcs, int, 10, 1
  dbo, TblWaferRma, FldFppDateTime, datetime, 23, 1
  dbo, TblWaferRma, FldFppRevision, varchar, 16, 1
  dbo, TblWaferRma, FldProbeFacility, varchar, 16, 1
  dbo, TblWaferRma, FldKeptByCustomer, varchar, 8, 1
  dbo, TblWaferRma, FldAccepted, varchar, 8, 1
  dbo, TblWaferRma, FldBack, varchar, 8, 1
  dbo, TblWaferRma, FldNote, varchar, 512, 1



  dbo, TblHistory, FldLot, varchar, 16, 1
  dbo, TblHistory, FldWafer, varchar, 16, 1
  dbo, TblHistory, FldFab, varchar, 4, 1
  dbo, TblHistory, FldArea, varchar, 16, 1
  dbo, TblHistory, FldInDateTime, datetime, 23, 1
  dbo, TblHistory, FldOutDateTime, datetime, 23, 1
  dbo, TblHistory, FldTraveler, varchar, 32, 1
  dbo, TblHistory, FldStep, varchar, 64, 1
  dbo, TblHistory, FldEquipment, varchar, 16, 1
  dbo, TblHistory, FldRecipe, varchar, 32, 1
  dbo, TblHistory, FldStepFlag, varchar, 32, 1
  dbo, TblHistory, FldSign, varchar, 32, 1



FldKind ????


 */



public class db
{
	Connection con2 = null;

	public Connection get_connection ()
	{   
		try {


			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			System.out.println("PROBE Driver reg OK...");

			//con2 = DriverManager.getConnection("jdbc:sqlserver://aiwymsdb;instanceName=SQLEXPRESS;database=Yms;user=ymsrdauser;password=ymsrdauser123");
			con2 = DriverManager.getConnection("jdbc:sqlserver://aiwymsdb;user=ymsrdauser;password=ymsrdauser123");
			Statement stmt = con2.createStatement();
			System.out.println("PROBE connection OK...");



		} catch (Exception exl) {
			System.out.println(" excp: " +exl.getMessage());
		}

		return(con2); 
	}   



	public void displayDbProperties(Connection con){
		java.sql.DatabaseMetaData dm = null;
		java.sql.ResultSet rs = null;


		try{

			if(con!=null){
				dm = con.getMetaData();
				System.out.println("Driver Information");
				System.out.println("\tDriver Name: "+ dm.getDriverName());
				System.out.println("\tDriver Version: "+ dm.getDriverVersion ());
				System.out.println("\nDatabase Information ");
				System.out.println("\tDatabase Name: "+ dm.getDatabaseProductName());
				System.out.println("\tDatabase Version: "+ dm.getDatabaseProductVersion());
				System.out.println("Avalilable Catalogs ");
				rs = dm.getCatalogs();
				while(rs.next()){
					System.out.println("\tcatalog: "+ rs.getString(1));
				} 
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}
		dm=null;
	}     




	public void coldesc(Connection con, String cat, String table){
		java.sql.DatabaseMetaData dm = null;
		java.sql.ResultSet res = null;


		try{

			if(con!=null){
				dm = con.getMetaData();


				res = dm.getColumns(cat, null, table, null);


				while(res.next()){
					System.out.println(
							"  "+res.getString("TABLE_SCHEM")
							+ ", "+res.getString("TABLE_NAME")
							+ ", "+res.getString("COLUMN_NAME")
							+ ", "+res.getString("TYPE_NAME")
							+ ", "+res.getInt("COLUMN_SIZE")
							+ ", "+res.getString("NULLABLE")); 


				} 
				res.close();
				res = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}
		dm=null;
	}     




	//select distinct FldState , FldFiscalYear , FldFiscalQuarter , FldPartTypeProbe , FldYieldConformingLimit from DbaLfoundryProduct.dbo.TblWcs where FldPartTypeProbe like 'C24C%'

	public String getWCSinfo(Connection con, String part){
		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select  FldState ,  FldPartTypeProbe, FldConformingLimit   " +
						" from DbaLfoundryProduct.dbo.TblWcs a "+
						" where" +
						" a.FldPartTypeProbe like '"+ part +"%'"; 	


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String pt = rs.getString("FldPartTypeProbe");
					String wcs = rs.getString("FldConformingLimit");
					String state = rs.getString("FldState");
					//String Y = rs.getString("FldFiscalYear");
					//String Q = rs.getString("FldFiscalQuarter");

					//System.out.println(Y+Q+ " " +  pt+ " WCS: "+ wcs + "  " + state);

					if(state.equals("Active")){
						ret = wcs;
						System.out.println( " " +  pt+ " WCS: "+ ret + "  " + state);
					}

					dim++;
				} 

				// System.out.println("probe data dimention= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}






	public SortedSet<String> getSteplist(Connection con, String lot,  String wafer){
		SortedSet<String> ret = new TreeSet<String>(); 
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select   a.FldStep   " +
						" from  DbalfoundryWafer.dbo.TblHistory  a "+
						" where" +
						" a.FldLot = '"+ lot +"'"+
						" and a.FldWafer = '"+ wafer+ "'" +
						" order by FldOutDateTime"; 	


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String Step = rs.getString("FldStep");

					//ret.add(Step);
					ret.add(Step);
					System.out.println("Step: "+ Step );


					dim++;
				} 

				// System.out.println("probe data dimension= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////    






	public String getEQPbystep(Connection con, String lot,  String wafer, String step){
		String ret= "NODATA";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select   a.FldStep , a.FldEquipment   " +
						" from  DbalfoundryWafer.dbo.TblHistory  a "+
						" where" +
						" a.FldLot = '"+ lot +"'"+
						" and a.FldWafer = '"+ wafer+ "'" +
						" and a.FldStep = '"+ step + "'" +
						" order by FldOutDateTime"; 	


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){

					String Equipment = rs.getString("FldEquipment");

					if(Equipment != null)
						ret = Equipment;
					System.out.println("Equip.: " + "	" + Equipment);


					dim++;
				} 

				// System.out.println("probe data dimension= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////    










	public String getRMAinfo(Connection con, String lot,  String wafer){
		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				//	String SQL_stmt = " select   a.FldCause , a.FldLevel , a.FldDefect , a.FldEquipment , a.FldArea, a.FldAccepted  " +
				String SQL_stmt = " select   a.FldCause , a.FldEquipment , a.FldArea, a.FldAccepted  " +
						" from  DbaLfoundryYt.dbo.TblAnalysis a "+
						" where" +
						" a.FldLot = '"+ lot +"'"+
						//" and a.FldStep = '"+ step +"'"+
						" and a.FldWafer = '"+ wafer+ "'" ; 	


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String cause = rs.getString("FldCause");
					//String Level = rs.getString("FldLevel");
					//String Defect = rs.getString("FldDefect");
					String acpt = rs.getString("FldAccepted");
					ret = acpt;
					System.out.println("RMA: "+ cause+acpt);


					dim++;
				} 

				// System.out.println("probe data dimension= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////    







	public String getbin(Connection con, String lot,  String wafer){
		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select   a.FldBin  " +
						" from  DbaLfoundryYt.dbo.TblAnalysis a "+
						" where" +
						" a.FldLot = '"+ lot +"'"+
						//" and a.FldStep = '"+ step +"'"+
						" and a.FldWafer = '"+ wafer+ "'" ; 	


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String bin = rs.getString("FldBin");
					ret = bin;
					System.out.println("BIN: "+ bin);


					dim++;
				} 

				// System.out.println("probe data dimension= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////    








	// FROM QA DB sono gli RMA tornati da ON
	public String getRMAacpt(Connection con, String lot,  String wafer){
		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select    a.FldAccepted  " +
						" from  DbaLfoundryQa.dbo.TblRma a "+
						" where" +
						" a.FldLot = '"+ lot +"'"+
						//" and a.FldStep = '"+ step +"'"+
						" and a.FldWafer = '"+ wafer+ "'" ; 	


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String acpt = rs.getString("FldAccepted");
					ret = acpt;
					System.out.println("RMA: "+ acpt);


					dim++;
				} 

				// System.out.println("probe data dimension= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}






	public String getDieType(Connection con, String lot, String wafer, String Di, String Dj){
		String ret = "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();
				//					DbaLfoundryProbe.dbo.TblDie
				String SQL_stmt = "SELECT FldKind from DbaLfoundryDie.dbo.TblKind " +
						"WHERE FldPartType = '" + getpartfromlot(con, lot) + "'" +
						" AND FldI= '" + Di + "'" +
						" AND FldJ= '" + Dj + "'" ; 	
				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					//						System.out.println("die flavor: "+ rs.getString(1) );
					ret = rs.getString(1);
					dim++;
				} 


				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	} 




	public String getfailbin(Connection con, String lot, String wafer,String step, String Di, String Dj){
		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select dp.FldFailBin  + dp.FldFailError as FldFail" + 
						" from  DbaLfoundryProbe.dbo.TblDie  dp"+
						" where" +
						" dp.FldLot = '"+ lot +"'"+
						" and dp.FldWafer = '"+ wafer+ "'"+ 
						" and dp.FldStep = '"+ step+ "'"+ 
						" and dp.FldI = '" + Di + "'"+ 
						" and dp.FldJ = '" + Dj + "'"+ 
						" and dp.FldRun = '0'" ; 	

				System.out.println(SQL_stmt);
				ResultSet rs = stmt.executeQuery(SQL_stmt);


				String die_type = getDieType(con,lot,wafer,Di,Dj);
				//					System.out.println("die_type: " + Di +","+Dj+"      "+ die_type);
				//					if(die_type.equals(".")||die_type.equals("<")||die_type.equals("#"))

				String bin = "UNK";
				if(rs.next())
					bin = rs.getString(1);
				else 
					//						if(! die_type.equals("v") && ! die_type.equals("p") && ! die_type.equals("X"))
					if( die_type.equals(".") )
						bin="..";

				ret=Di+","+Dj+":"+bin;

				System.out.println("die:" + Di +","+Dj+  " fail bin: "+ bin + " type:" + die_type);


				/*
					int dim =0;
					while(rs.next()){
						String bin = rs.getString(1);

						if(die_type.equals("v") && bin == null)
							bin="..";

						ret = bin;
						System.out.println("die:" + Di +","+Dj+  "fail bin/type: "+ bin + " " + die_type);


						dim++;
					} 
				 */
				// System.out.println("probe data dimention= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////     	


	/*
		public String getfailbin(Connection con, String lot, String step, String wafer, String Di, String Dj){
			String ret= "null";
			try{

				if(con!=null){
					Statement stmt = con.createStatement();

					String SQL_stmt = " select  l.FldStep  , " +
							" l.FldLot  ," +
							" l.FldDevice  , " +
							" l.FldPartType  , " +
							" w.FldPartType  , " +
							" w.FldStepOutDateTime  , " +
							" w.FldRun, " +
							" d.FldI  ," +
							" d.FldJ  ," +
							" d.FldFailBin  ," +
							" d.FldFailError  ," +
							" d.FldFailBinOut " +
							" from DbaLfoundryProbe.dbo.TblLot l inner join  DbaLfoundryProbe.dbo.TblWafer w on " +
							" (w.FldLot = l.FldLot and w.FldStep = l.FldStep) inner join   DbaLfoundryProbe.dbo.TblDie d on " +
							" (d.FldLot = w.FldLot and d.FldStep = w.FldStep and d.FldWafer = w.FldWafer)"+
							" where" +
							" l.FldLot = '"+ lot +"'"+
							" and w.FldStep = '"+ step +"'"+
							" and w.FldWafer = '"+ wafer+ "'"+ 
							" and d.FldI = " + Di +
							" and d.FldJ = " + Dj ; 	


					System.out.println(SQL_stmt);	


					ResultSet rs = stmt.executeQuery(SQL_stmt);


					int dim =0;
					while(rs.next()){
						String bin = rs.getString("FldFailBinOut");
						System.out.println("data= "+ bin);
						ret = bin;

						dim++;
					} 

					// System.out.println("probe data dimention= "+ dim);
					rs.close();
					rs = null;

				}else System.out.println("Error: No active Connection");
			}catch(Exception e){
				e.printStackTrace();
			}

			return(ret);
		}
		/////////////////////////////////////////     

	 */	





	public String getMAXrun(Connection con, String lot, String step, String wafer){
		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select  " +

							" w.FldRun " +
							" from   DbaLfoundryProbe.dbo.TblWafer w " +
							" where" +
							" w.FldLot = '"+ lot +"'"+
							" and w.FldStep in ("+ step +")" +
							" and w.FldWafer = '"+ wafer+ "'";	


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String runid = rs.getString("FldRun");
					System.out.println("  run "+ runid);
					ret = runid;
					dim++;
				} 

				// System.out.println("probe data dimention= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////     



	public List<String> getDieList(Connection con, String lot, String wafer){
		//			String ret = "null";
		List<String> dielist = new ArrayList<String>();
		try{

			if(con!=null){
				Statement stmt = con.createStatement();
				//					DbaLfoundryProbe.dbo.TblDie
				String SQL_stmt = "SELECT FldKind, FldI, FldJ from DbaLfoundryDie.dbo.TblKind " +
						"WHERE FldPartType = '" + getpartfromlot(con, lot) + "'" ;


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){


					//						ret = rs.getString(1);
					String i = rs.getString("FldI");
					String j = rs.getString("FldJ");
					System.out.println("die  "+ i+","+j + " type:"+rs.getString(1) );
					dielist.add(i+","+j);
					dim++;
				} 


				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(dielist);
	} 




	public String getwfails(Connection con, String lot, String step, String wafer, String run, Frequency f, Frequency gf, Frequency mapf){

		//Frequency f = new Frequency();
		//Set<String> binlist = new HashSet<String>();

		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select " +
						" d.FldI  ," +
						" d.FldJ  ," +
						" d.FldFailBin  ," +
						" d.FldFailError  ," +
						" d.FldFailBinOut " +
						" from  DbaLfoundryProbe.dbo.TblDie d  " +
						" where" +
						" d.FldLot = '"+ lot +"'"+
						//" and d.FldStep = '"+ step +"'"+
						" and d.FldStep in ("+ step +")" +
						" and d.FldRun = '"+ run +"'" +
						" and d.FldWafer = '"+ wafer+ "'";							


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String i = rs.getString("FldI");
					String j = rs.getString("FldJ");
					String bin = rs.getString("FldFailBin");
					String err = rs.getString("FldFailError");
					String binerr = bin+err;

					mapf.addValue(i+","+j);
					f.addValue(binerr);
					gf.addValue(binerr);
					//System.out.println("data= "+ bin);
					//System.out.println(lot + "," + wafer + "," + bin+":"+err);
					ret = bin;

					dim++;
				} 

				// System.out.println("probe data dimension= "+ dim);
				rs.close();
				rs = null;


				//System.out.println(f.getCumPct("Aq"));

				/*
					Iterator<Comparable<?>> it = f.valuesIterator() ;

					while(it.hasNext() ){
						String bbb = (String) it.next();
						System.out.println(bbb + ": " + f.getPct(bbb));

					}
				 */

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////     





	public int getmaxdpw(Connection con, String part, String step){
		int maxdpw = 0;
		try{

			if(con!=null){
				Statement stmt = con.createStatement();
				//					DbaLfoundryProbe.dbo.TblDie
				String SQL_stmt = "SELECT FldKind from DbaLfoundryDie.dbo.TblKind " +
						"WHERE FldPartType = '" + part + "'" ; 	
				ResultSet rs = stmt.executeQuery(SQL_stmt);

				while(rs.next()){
					//						System.out.println("die flavor: "+ rs.getString(1) );
					String res = rs.getString(1);
					if(res.equals("."))
						maxdpw++;
				} 


				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		maxdpw = maxdpw +1;
		
		// TODO Need to overwrite the std MAP in some cases of reduced mamp, DIEIN not present in YMS DB...
		if(part.startsWith("C25D") && step.equals("'PPP'"))
			maxdpw  = 155;
		
		
		System.out.println("Max dpw for " + part + ": " + maxdpw);
		return(maxdpw);
	} 		




	public String getpartfromlot(Connection con, String lot){

		String part = "null";

		try{

			if(con!=null){
				Statement stmt = con.createStatement();
				String SQL_stmt = "SELECT FldPartType from DbaLfoundryProbe.dbo.TblLot " +
						"WHERE FldLot = '"+ lot +"'" ; 

				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					//   System.out.println("part "+ rs.getString(1));
					part = rs.getString(1);
					dim++;
				} 

				//System.out.println("probe data dimention= "+ dim);
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return part;
	}  






	// To add bin maps...
	public String getwfails2(Connection con, String lot, String step, String wafer, String run, Frequency f, Frequency gf, Frequency mapf,Frequency wffm, Object[][] binmap, String y_pct){

		//Frequency f = new Frequency();
		//Set<String> binlist = new HashSet<String>();

		String ret= "null";
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select " +
						" d.FldI  ," +
						" d.FldJ  ," +
						" d.FldFailBin  ," +
						" d.FldFailError  ," +
						" d.FldFailBinOut " +
						" from  DbaLfoundryProbe.dbo.TblDie d  " +
						" where" +
						" d.FldLot = '"+ lot +"'"+
						//" and d.FldStep = '"+ step +"'"+
						" and d.FldStep in ("+ step +")" +
						" and d.FldRun = '"+ run +"'" +
						" and d.FldWafer = '"+ wafer+ "'";							


				System.out.println(SQL_stmt);	


				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String i = rs.getString("FldI");
					String j = rs.getString("FldJ");
					String bin = rs.getString("FldFailBin");
					String err = rs.getString("FldFailError");
					String binerr = bin+err;


					if(!binerr.equals("..") && !binerr.equals(".*")) {  // added on 8/14 to remove .. and .* from bin list
						mapf.addValue(i+","+j);
						wffm.addValue(i+","+j+","+binerr);
						f.addValue(binerr);
						gf.addValue(binerr);

						// Fill frequency map for each bin...
						for(int k =0; k < binmap.length; k++){
							if(binmap[k][0].equals(binerr)) {
								((Frequency)binmap[k][1]).addValue(i+","+j);

								if(y_pct.equals("p25"))
									((Frequency)binmap[k][2]).addValue(i+","+j);
								if(y_pct.equals("p75"))
									((Frequency)binmap[k][3]).addValue(i+","+j);
							}
						}
					}



					ret = bin;

					dim++;
				} 

				// System.out.println("probe data dimension= "+ dim);
				rs.close();
				rs = null;


			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}
	/////////////////////////////////////////     






	public Frequency get_dev(Connection con,  String time) {
		Frequency step_freq =  new Frequency();
		System.out.println("Device autoselection... ");

		try{
			if(con!=null){
				Statement stmt = con.createStatement();

				String step = "'PPP','FPP','QPP'";
				//					String step = "'PPP','SPP'";
				//					String step = "'SPP'";

				String SQL_stmt = " select  " +
						" w.FldStep  , " +
						" w.FldPartType  , " +
						" w.FldStepOutDateTime   " +
						" from   DbaLfoundryProbe.dbo.TblWafer w " +
						" where" +
						" w.FldStepOutDateTime >= Cast('" + time + "' as datetime)"  + 
						" and w.FldStep in ("+ step +")" +
						" and w.FldRun = '"+ 0 +"'" +
						" order by FldStepOutDateTime" 
						;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String part = rs.getString("FldPartType");
					String mystep = rs.getString("FldStep");
					String date = rs.getString("FldStepOutDateTime");

					String ps = part+"-"+ "'"+mystep+"'";
					//						System.out.println("Adding: " + ps);
					step_freq.addValue(ps);

					dim++;
				} 

				// System.out.println("probe data dimention= "+ dim);

				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}



		return step_freq;
	}








	public String getwlist(Connection con, String dev, String step, String time){
		String ret= "null";
		try{
			File logfile = new File("list.txt");
			FileWriter log = new FileWriter(logfile);

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select  " +
						" w.FldWafer  , " +
						" w.FldLot  , " +
						" w.FldStepOutDateTime   " +
						" from   DbaLfoundryProbe.dbo.TblWafer w " +
						" where" +
						" w.FldPartType LIKE '"+ dev + "%' " +
						" and w.FldStepOutDateTime >= Cast('" + time + "' as datetime)"  + 
						" and w.FldStep in ("+ step +")" +
						" and w.FldRun = '"+ 0 +"'" +
						" order by FldStepOutDateTime" 
						;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String lot = rs.getString("FldLot");
					String wf = rs.getString("FldWafer");
					String date = rs.getString("FldStepOutDateTime");

					ret = lot;

					String last = getMAXrun(con, lot, step, wf);
					//System.out.println("Last run is :" + last);

					log.write(lot+","+wf+","+last+","+ date+"\n");				
					log.flush();
					dim++;
				} 

				// System.out.println("probe data dimention= "+ dim);
				log.close();
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}










	public String getwdpw2(Connection con, String lot, String wf, String run, String date,  String step, FileWriter log){
		String ret= "null";
		try{
			//	File logfile = new File("data.txt");
			//FileWriter log = new FileWriter(logfile);

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select  " +
						" w.FldPartType  , " +
						" w.FldStation  , " +
						" w.FldPassDpw  , " +
						" w.FldProgramRevision  , " +
						" w.FldProbeFacility  , " +
						" w.FldStepOutDateTime " +
						" from   DbaLfoundryProbe.dbo.TblWafer w " +
						" where" +
						//							" w.FldPartType LIKE '"+ dev + "%' " +
						//							" and w.FldStepOutDateTime >= Cast('" + time + "' as datetime)"  + 
						//" and w.FldStep = '"+ step +"'" +

							" w.FldLot = '"+ lot +"'"+
							" and w.FldWafer = '"+ wf+ "'"+							
							" and w.FldStep in ("+ step +")" +
							" and w.FldRun = '"+ run +"'" +
							" order by FldStepOutDateTime" 
							;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String prev = rs.getString("FldProgramRevision");
					String dpw = rs.getString("FldPassDpw");
					//String date = rs.getString("FldStepOutDateTime");
					//						String part = rs.getString("FldPartType");
					String part = rs.getString("FldStation");
					String pfac = rs.getString("FldProbeFacility");

					System.out.println(lot + "," + wf + "," + dpw + "," + date);
					ret = lot;


					log.write(dpw+","+ date +","+ prev +"," + get_ww(date)+","+lot+","+wf+","+part+ ","+pfac+  ","+run+"\n");

					// ANOVA on PART
					//log.write(dpw+","+ date +","+ part +"," + get_ww(date)+"\n");
					log.flush();

					dim++;
				} 

				// System.out.println("probe data dimention= "+ dim);
				//			log.close();
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}












	public String getwdpw(Connection con, String dev, String step, String time){
		String ret= "null";
		try{
			File logfile = new File("data.txt");
			FileWriter log = new FileWriter(logfile);

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = " select  " +
						" w.FldWafer  , " +
						" w.FldLot  , " +
						" w.FldRun , " +
						" w.FldPartType  , " +
						" w.FldPassDpw  , " +
						" w.FldProgramRevision  , " +
						" w.FldProbeFacility  , " +
						" w.FldStepOutDateTime " +
						" from   DbaLfoundryProbe.dbo.TblWafer w " +
						" where" +
						" w.FldPartType LIKE '"+ dev + "%' " +
						" and w.FldStepOutDateTime >= Cast('" + time + "' as datetime)"  + 
						//" and w.FldStep = '"+ step +"'" +
						" and w.FldStep in ("+ step +")" +
						" and w.FldRun = '"+ 0 +"'" +
						" order by FldStepOutDateTime" 
						;



				//where l.FldStep in ('FPP','FPXP')


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);


				int dim =0;
				while(rs.next()){
					String lot = rs.getString("FldLot");
					String prev = rs.getString("FldProgramRevision");
					String wf = rs.getString("FldWafer");
					String dpw = rs.getString("FldPassDpw");
					String date = rs.getString("FldStepOutDateTime");
					String part = rs.getString("FldPartType");
					String pfac = rs.getString("FldProbeFacility");

					System.out.println(lot + "," + wf + "," + dpw + "," + date);
					ret = lot;

					String last = getMAXrun(con, lot, step, wf);
					//System.out.println("Last run is :" + last);

					log.write(dpw+","+ date +","+ prev +"," + get_ww(date)+","+lot+","+wf+","+part+ ","+pfac+  ","+last+"\n");

					// ANOVA on PART
					//log.write(dpw+","+ date +","+ part +"," + get_ww(date)+"\n");
					log.flush();

					dim++;
				} 

				// System.out.println("probe data dimention= "+ dim);
				log.close();
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(ret);
	}



	public String get_ww(String it) throws ParseException{
		DateFormat df = new SimpleDateFormat ("yyyy-MM-dd");
		//			SimpleDateFormat formatter;
		//			formatter = new SimpleDateFormat("yw");
		Date cdate = df.parse(it);


		Calendar cal = new GregorianCalendar();
		cal.setTime(cdate);
		cal.setFirstDayOfWeek(Calendar.FRIDAY);
		//			cal.set(Calendar.HOUR_OF_DAY, 19);

		String myww = Integer.toString(cal.getWeekYear()) + Integer.toString(cal.get(Calendar.WEEK_OF_YEAR));
		//System.out.println(formatter.format(cdate));

		return(myww);


		//			return(formatter.format(cdate));


	}	



	
//	dbo, TblWafer, FldStep, varchar, 8, 0
//	dbo, TblWafer, FldRun, varchar, 8, 0
//	dbo, TblWafer, FldLot, varchar, 16, 0
//	dbo, TblWafer, FldWafer, varchar, 8, 0
//	dbo, TblWafer, FldPartType, varchar, 16, 1
//	dbo, TblWafer, FldStepInDateTime, datetime, 23, 1
//	dbo, TblWafer, FldStepOutDateTime, datetime, 23, 1
//	dbo, TblWafer, FldStepState, varchar, 16, 1
//	dbo, TblWafer, FldMapVersion, varchar, 32, 1
//	dbo, TblWafer, FldProgram, varchar, 32, 1
//	dbo, TblWafer, FldProgramRevision, varchar, 16, 1
//	dbo, TblWafer, FldStation, varchar, 16, 1
//	dbo, TblWafer, FldProbeCard, varchar, 16, 1
//	dbo, TblWafer, FldPassDpw, int, 10, 1
//	dbo, TblWafer, FldNote, varchar, 256, 1
	
	// WAFER LEVEL...
	public Table get_yield_by_week(Connection con, String step, String dev) {

		Table yield = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String time = "2020/09/01";

				// set first day of the week to Friday = 5;
				boolean rs22  = stmt.execute( "SET DATEFIRST 5 ;  ");


				String SQL_stmt = " select  " +
						" AVG(FldPassDpw) as Yield  , " +
						" COUNT(FldPassDpw) as WfCount  , " +
						" DATEPART(year, FldStepInDateTime) as Year , " + 
						" DATEPART(week, FldStepInDateTime) as WW, " + 
						" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(week, FldStepInDateTime)) as YearWorkWeek "+
						" from  DbaLfoundryProbe.dbo.TblWafer " +
						" where " +
						" FldStepInDateTime >= Cast('" + time + "' as datetime)"  + 
						" and FldStep in ("+ step +")" +
						" and FldPartType = '"+ dev + "'" +
						" GROUP BY DATEPART(year, FldStepInDateTime), DATEPART(week, FldStepInDateTime)"  +
						" ORDER BY DATEPART(year, FldStepInDateTime), DATEPART(week, FldStepInDateTime)"
						;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				yield = Table.read().db(rs, "Yield");	
				System.out.println(yield.print());

				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(yield);
	}
	
	
	
	
	

/*
	public Table get_yield_by_week(Connection con, String step, String dev) {

		Table yield = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String time = "2020/09/01";

				// set first day of the week to Friday = 5;
				boolean rs22  = stmt.execute( "SET DATEFIRST 5 ;  ");


				String SQL_stmt = " select  " +
						" AVG(FldYieldMeanDpw) as Yield  , " +
						" COUNT(FldYieldMeanDpw) as LotCount  , " +
						" DATEPART(year, FldStepInDateTime) as Year , " + 
						" DATEPART(week, FldStepInDateTime) as WW, " + 
						" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(week, FldStepInDateTime)) as YearWorkWeek "+
						" from  DbaLfoundryProbe.dbo.TblLot " +
						" where " +
						" FldStepInDateTime >= Cast('" + time + "' as datetime)"  + 
						" and FldStep in ("+ step +")" +
						" and FldPartType = '"+ dev + "'" +
						" GROUP BY DATEPART(year, FldStepInDateTime), DATEPART(week, FldStepInDateTime)"  +
						" ORDER BY DATEPART(year, FldStepInDateTime), DATEPART(week, FldStepInDateTime)"
						;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				yield = Table.read().db(rs, "Yield");	
				System.out.println(yield.print());

				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(yield);
	}

*/




	public Table get_yield_by_quarter(Connection con, String step, String dev) {

		Table yield = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String time = "2019/01/01";

				// set first day of the week to Friday = 5;
//				boolean rs22  = stmt.execute( "SET DATEFIRST 5 ;  ");


				String SQL_stmt = 
						" select  " +
								" ROUND( AVG(FldYieldMeanDpw), 1 ) as Yield  , " +
								//								" AVG(FldYieldMeanDpw) as Yield  , " +
								" COUNT(FldYieldMeanDpw) as LotCount  , " +
								" DATEPART(year, FldStepInDateTime) as Year , " + 
								" DATEPART(quarter, FldStepInDateTime) as QTR, " + 
								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(quarter, FldStepInDateTime)) as YearQTR "+
								" from  DbaLfoundryProbe.dbo.TblLot " +
								" where " +
								" FldStepInDateTime >= Cast('" + time + "' as datetime)"  + 
								" and FldStep in ("+ step +")" +
								" and FldPartType = '"+ dev + "'" +
								" GROUP BY DATEPART(year, FldStepInDateTime), DATEPART(quarter, FldStepInDateTime)"  +
								" ORDER BY DATEPART(year, FldStepInDateTime), DATEPART(quarter, FldStepInDateTime)"
								;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				yield = Table.read().db(rs, "Yield");	
				System.out.println(yield.print());

				rs.close();
				rs = null;

			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return(yield);
	}




	/*
	public Table get_yield_by_month(Connection con, String step, String dev, String yearmonth) {

		Table yield = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

//				String time = "2018/01/01";
//
//				// set first day of the week to Friday = 5;
//				boolean rs22  = stmt.execute( "SET DATEFIRST 5 ;  ");


				String SQL_stmt = 
						" select  " +
//								" ROUND( AVG(FldYieldMeanDpw), 2 ) as Yield  , " +
											" AVG(FldYieldMeanDpw) as Yield  , " +
								" COUNT(FldYieldMeanDpw) as LotCount  , " +
								" DATEPART(year, FldStepInDateTime) as Year , " + 
								" DATEPART(month, FldStepInDateTime) as Month, " + 
								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(Month, FldStepInDateTime)) as YearMonth "+
								" from  DbaLfoundryProbe.dbo.TblLot " +
								" where " +
								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(Month, FldStepInDateTime)) = " + yearmonth + 
								" and FldStep in ("+ step +")" +
								" and FldPartType = '"+ dev + "'" +
								" GROUP BY DATEPART(year, FldStepInDateTime), DATEPART(Month, FldStepInDateTime)"  +
								" ORDER BY DATEPART(year, FldStepInDateTime), DATEPART(Month, FldStepInDateTime)"
								;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				yield = Table.read().db(rs, "Yield");	
				System.out.println(yield.print());

				rs.close();
				rs = null;

			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return(yield);
	}
*/
	
	
	
//	dbo, TblWafer, FldStep, varchar, 8, 0
//	dbo, TblWafer, FldRun, varchar, 8, 0
//	dbo, TblWafer, FldLot, varchar, 16, 0
//	dbo, TblWafer, FldWafer, varchar, 8, 0
//	dbo, TblWafer, FldPartType, varchar, 16, 1
//	dbo, TblWafer, FldStepInDateTime, datetime, 23, 1
//	dbo, TblWafer, FldStepOutDateTime, datetime, 23, 1
//	dbo, TblWafer, FldStepState, varchar, 16, 1
//	dbo, TblWafer, FldMapVersion, varchar, 32, 1
//	dbo, TblWafer, FldProgram, varchar, 32, 1
//	dbo, TblWafer, FldProgramRevision, varchar, 16, 1
//	dbo, TblWafer, FldStation, varchar, 16, 1
//	dbo, TblWafer, FldProbeCard, varchar, 16, 1
//	dbo, TblWafer, FldPassDpw, int, 10, 1
//	dbo, TblWafer, FldNote, varchar, 256, 1
	public Table get_yield_by_period(Connection con, String step, String dev, String yearmonth) {

		Table yield = null; 
		String time[] = get_starting_date(yearmonth);
		
		
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = 
						" select  " +
//								" ROUND( AVG(FldYieldMeanDpw), 2 ) as Yield  , " +
								" AVG(FldPassDpw) as Yield  , " +
								" COUNT(FldPassDpw) as WfsCount   " +
//								" DATEPART(year, FldStepInDateTime) as Year , " + 
//								" DATEPART(month, FldStepInDateTime) as Month " + 
//								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(Month, FldStepInDateTime)) as YearMonth "+
								" from  DbaLfoundryProbe.dbo.TblWafer " +
								" where " +
								" FldStepOutDateTime >= Cast('" + time[0] + "' as datetime)"  + 
								" and FldStepOutDateTime <= Cast('" + time[1] + "' as datetime)"  + 
//								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(Month, FldStepInDateTime)) = " + yearmonth + 
								" and FldStep in ("+ step +")" +
								" and FldPartType = '"+ dev + "'" 
								
//								" GROUP BY DATEPART(year, FldStepInDateTime), DATEPART(Month, FldStepInDateTime)"  +
//								" ORDER BY DATEPART(year, FldStepInDateTime), DATEPART(Month, FldStepInDateTime)"
								;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				yield = Table.read().db(rs, "Yield");	
				System.out.println(yield.print());

				rs.close();
				rs = null;

			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return(yield);
	}

	
	
	
	
	
	public Table get_yield_by_month(Connection con, String step, String dev) {

		Table yield = null; 
		String time = "2021/01/01";
		
		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_stmt = 
						" select  " +
								" ROUND( AVG(FldPassDpw), 2 ) as Yield  , " +
//								" AVG(FldPassDpw) as Yield  , " +
								" COUNT(FldPassDpw) as WfsCount  , " +
								" DATEPART(year, FldStepInDateTime) as Year , " + 
								" DATEPART(month, FldStepInDateTime) as Month, " + 
								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(Month, FldStepInDateTime)) as YearMonth "+
								" from  DbaLfoundryProbe.dbo.TblWafer " +
								" where " +
								" FldStepInDateTime >= Cast('" + time + "' as datetime)"  + 
								" and FldStep in ("+ step +")" +
								" and FldPartType = '"+ dev + "'" +
								" GROUP BY DATEPART(year, FldStepInDateTime), DATEPART(Month, FldStepInDateTime)"  +
								" ORDER BY DATEPART(year, FldStepInDateTime), DATEPART(Month, FldStepInDateTime)"
								;


				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				yield = Table.read().db(rs, "Yield");	
				System.out.println(yield.print());

				rs.close();
				rs = null;

			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return(yield);
	}
	
	



	public String get_lots_by_qtr(Connection con, String step, String dev, String yearqtr) {
		String sql_lots="(";

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_lot = 
						" select  " +
								" FldLot as Lot , " +
								" DATEPART(year, FldStepOutDateTime) as Year , " + 	
								" DATEPART(quarter, FldStepOutDateTime) as QTR, " +
								" DATEPART(month, FldStepOutDateTime) as Month , " + 
								" DATEPART(week, FldStepOutDateTime) as WW , " + 
								" CONCAT(DATEPART(year, FldStepOutDateTime),DATEPART(quarter, FldStepOutDateTime)) as YearQTR "+

							" from  DbaLfoundryProbe.dbo.TblLot " +

							" where " +
							" CONCAT(DATEPART(year, FldStepOutDateTime),DATEPART(quarter, FldStepOutDateTime)) = " +yearqtr + 
							" and FldStep in ("+ step +")" +
							" and FldPartType = '"+ dev + "'" 
							;


				ResultSet rs1 = stmt.executeQuery(SQL_lot);
				Table lot = Table.read().db(rs1, "Lots");
				System.out.println(lot.print());

				for(Row r : lot) {
					sql_lots = sql_lots+ "'" + (r.getText("Lot")) +"'"+"," ;
				}
				sql_lots = sql_lots.substring(0, sql_lots.length() - 1)+")";
				System.out.println(sql_lots);

			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return sql_lots;

	}







	// TODO Need to add a consistency check between PASSDPW and wafer vs count of .. @ BIN LEVEL...
	public Table get_lots_by_month_2(Connection con, String step, String dev,String yearmonth) {

		Table lot = Table.create("LOTS");

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_lot = 
						" select  " +
								" FldLot as Lot, " +
								" FldWafer as wf, " +
								" FldRun as Run, " +
								" FldPassDpw PDPW,"+
								" CONCAT (FldLot,'-',FldWafer,'-',FldRun) as lwr,"+
								" DATEPART(year, FldStepInDateTime) as Year , " + 	
								" DATEPART(quarter, FldStepInDateTime) as QTR, " +
								" DATEPART(month, FldStepInDateTime) as Month , " + 
								" DATEPART(week, FldStepInDateTime) as WW , " + 
								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(month, FldStepInDateTime)) as YearMonth "+

							" from  DbaLfoundryProbe.dbo.TblWafer " +

							" where " +
							" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(month, FldStepInDateTime)) = " +yearmonth + 
							" and FldStep in ("+ step +")" +
							" and FldPartType = '"+ dev + "'"  
							;

				System.out.println(SQL_lot);
				ResultSet rs1 = stmt.executeQuery(SQL_lot);
				lot = Table.read().db(rs1, "Lots");
//				System.out.println(lot.print());


			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return lot;
	}



	

	public  String[] get_starting_date(String scenario){

		String[] date = new String[2];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime start;
		LocalDateTime end;

		System.out.println(scenario);
		
		if(scenario == "C30") { //last 30 days
			start = LocalDateTime.now().minusDays(30);
			end = LocalDateTime.now();
			date[0] = start.format(formatter);
			date[1] = end.format(formatter);
		}

		if(scenario == "P30") { //last 30 days
			start = LocalDateTime.now().minusDays(60);
			end = LocalDateTime.now().minusDays(30);;
//			date[0] = dateFormat.format(start);
			date[0] = start.format(formatter);
			date[1] = end.format(formatter);
		}
		
		
		if(scenario == "C7") { //last 30 days
			start = LocalDateTime.now().minusDays(7);
			end = LocalDateTime.now();
			date[0] = start.format(formatter);
			date[1] = end.format(formatter);
		}
		
		
		if(scenario == "P7") { //last 30 days
			start = LocalDateTime.now().minusDays(14);
			end = LocalDateTime.now().minusDays(7);;
//			date[0] = dateFormat.format(start);
			date[0] = start.format(formatter);
			date[1] = end.format(formatter);
		}
		


		return date;		
	}

	
	
	// TODO Need to add a consistency check between PASSDPW and wafer vs count of .. @ BIN LEVEL...
	public Table get_lots_by_period(Connection con, String step, String dev,String yearmonth) {

		Table lot = Table.create("LOTS");
		
		String time[] = get_starting_date(yearmonth);

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_lot = 
						" select  " +
								" FldLot as Lot, " +
								" FldWafer as wf, " +
								" FldRun as Run, " +
								" FldPassDpw PDPW,"+
								" CONCAT (FldLot,'-',FldWafer,'-',FldRun) as lwr,"+
								" DATEPART(year, FldStepInDateTime) as Year , " + 	
								" DATEPART(quarter, FldStepInDateTime) as QTR, " +
								" DATEPART(month, FldStepInDateTime) as Month , " + 
								" DATEPART(week, FldStepInDateTime) as WW , " + 
								" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(month, FldStepInDateTime)) as YearMonth "+

							" from  DbaLfoundryProbe.dbo.TblWafer " +

							" where " +
							" FldStepOutDateTime >= Cast('" + time[0] + "' as datetime)"  + 
							" and FldStepOutDateTime <= Cast('" + time[1] + "' as datetime)"  + 
//							" CONCAT(DATEPART(year, FldStepInDateTime),DATEPART(month, FldStepInDateTime)) = " +yearmonth + 
							" and FldStep in ("+ step +")" +
							" and FldPartType = '"+ dev + "'"  
							;


				System.out.println(SQL_lot);
				ResultSet rs1 = stmt.executeQuery(SQL_lot);
				lot = Table.read().db(rs1, "Lots");
//				System.out.println(lot.print());


			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return lot;
	}
	
	
	
	

/*
	public String get_lots_by_month(Connection con, String step, String dev,String yearmonth) {
		String sql_lots="(";

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String SQL_lot = 
						" select  " +
								" FldLot as Lot , " +
								" DATEPART(year, FldStepOutDateTime) as Year , " + 	
								" DATEPART(quarter, FldStepOutDateTime) as QTR, " +
								" DATEPART(month, FldStepOutDateTime) as Month , " + 
								" DATEPART(week, FldStepOutDateTime) as WW , " + 
								" CONCAT(DATEPART(year, FldStepOutDateTime),DATEPART(month, FldStepOutDateTime)) as YearMonth "+

							" from  DbaLfoundryProbe.dbo.TblLot " +

							" where " +
							" CONCAT(DATEPART(year, FldStepOutDateTime),DATEPART(month, FldStepOutDateTime)) = " +yearmonth + 
							" and FldStep in ("+ step +")" +
							" and FldPartType = '"+ dev + "'" 
							;


				ResultSet rs1 = stmt.executeQuery(SQL_lot);
				Table lot = Table.read().db(rs1, "Lots");
				//				System.out.println(lot.print());

				for(Row r : lot) {
					sql_lots = sql_lots+ "'" + (r.getText("Lot")) +"'"+"," ;
				}
				sql_lots = sql_lots.substring(0, sql_lots.length() - 1)+")";
				//				System.out.println(sql_lots);

			} else System.out.println("Error: No active Connection");
		} catch(Exception e){
			e.printStackTrace();
		}

		return sql_lots;

	}
*/


	
	
	
	//		dbo, TblDie, FldStep, varchar, 8, 0
	//		dbo, TblDie, FldRun, varchar, 8, 0
	//		dbo, TblDie, FldLot, varchar, 16, 0
	//		dbo, TblDie, FldWafer, varchar, 8, 0
	//		dbo, TblDie, FldI, int, 10, 0
	//		dbo, TblDie, FldJ, int, 10, 0
	//		dbo, TblDie, FldFailBin, varchar, 1, 1
	//		dbo, TblDie, FldFailError, varchar, 1, 1
	//		dbo, TblDie, FldFailBinOut, varchar, 64, 1
	//		dbo, TblDie, FldNote, varchar, 128, 1

	public Table get_bin_pareto(Connection con, String step, String dev) {

		Table binpareto = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();


				// Lot of July...
				String sql_lots = "";



				String SQL_stmt = " select  " +
						" CONCAT (FldFailBin,FldFailError) as bin, " +

							" COUNT(CONCAT (FldFailBin,FldFailError)) as bincount " +
							" from  DbaLfoundryProbe.dbo.TblDie " +
							" where " +
//							" FldLot IN " + sql_lots + 
							"  CONCAT(FldLot,FldWafer) IN  ('9211539.00909')" +
							//														" FldLot IN  ('9211539.009')" + 
							" and FldRun = 0" +
							" and FldStep in ("+ step +")" +
							" GROUP BY CONCAT (FldFailBin,FldFailError)"  
							;



				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				binpareto = Table.read().db(rs, "Bin Pareto");	
				System.out.println(binpareto.print(10));
				//
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(binpareto);
	}




//	dbo, TblDie, FldStep, varchar, 8, 0
//	dbo, TblDie, FldRun, varchar, 8, 0
//	dbo, TblDie, FldLot, varchar, 16, 0
//	dbo, TblDie, FldWafer, varchar, 8, 0
//	dbo, TblDie, FldI, int, 10, 0
//	dbo, TblDie, FldJ, int, 10, 0
//	dbo, TblDie, FldFailBin, varchar, 1, 1
//	dbo, TblDie, FldFailError, varchar, 1, 1
//	dbo, TblDie, FldFailBinOut, varchar, 64, 1
//	dbo, TblDie, FldNote, varchar, 128, 1
	public Table get_bin_pareto_2(Connection con, String step, String dev, String sql_lots) {

		Table binpareto = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

//				COLLATE Latin1_General_CS_AS
				
				String SQL_stmt = " select  " +
//							" CONCAT (FldFailBin,FldFailError) as bin, " +
							" CONCAT (FldFailBin COLLATE Latin1_General_CS_AS ,FldFailError COLLATE Latin1_General_CS_AS) as bin, " +
							" COUNT(CONCAT (FldFailBin,FldFailError)) as bincount " +
							" from  DbaLfoundryProbe.dbo.TblDie " +
							" where " +
							" FldLot IN " + sql_lots + 
							" and FldStep in ("+ step +")" +
							" GROUP BY CONCAT (FldFailBin COLLATE Latin1_General_CS_AS,FldFailError COLLATE Latin1_General_CS_AS) " 
							;

				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
//				binpareto = Table.read().db(rs, "Bin Pareto");
				

				
				//////////////////// case insensitive bins in Table saw ISSUE.../////////////////////////////
				StringColumn bin = StringColumn.create("bin");
				IntColumn bincount = IntColumn.create("bincount");
				while(rs.next()){
					String mybin = rs.getString("bin");
					String bincode = mybin+" ("+mybin.hashCode()+")";
					bin.append(bincode);
					int count = rs.getInt("bincount");
					bincount.append(count);
				}
				binpareto = Table.create("Bin Pareto");
				binpareto.addColumns(bin);
				binpareto.addColumns(bincount);
				////////////////////////////////////////////////////////

				
				
				
				System.out.println(binpareto.print(30));
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(binpareto);
	}

	
	
	
	
	
	
	

/*
	public String get_die_yield(Connection con, String step, String dev,  String lwr) {

		String dieyield = "-1";
		Table binpareto = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String[] mylwr = lwr.split("-");





				String SQL_stmt = " select  " +
						" CONCAT (FldFailBin,FldFailError) as bin, " +
						" COUNT(CONCAT (FldFailBin,FldFailError)) as bincount " +
						" from  DbaLfoundryProbe.dbo.TblDie " +
						" where " +
						//							" CONCAT(FldLot,FldWafer,FldRun) IN " + sql_lots + 
						//							" CONCAT(FldLot,FldWafer,FldRun) = '"+  lwr +"'" + 
						" FldLot = " + "'"+  mylwr[0] +"'" +
						" and FldWafer = " + "'"+  mylwr[1] +"'" +
						" and FldRun = " + "'"+  mylwr[2] +"'" +
						//							" and FldRun = 1" +
						" and FldStep in ("+ step +")" +
						" GROUP BY CONCAT (FldFailBin,FldFailError)"  
						;



//				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				binpareto = Table.read().db(rs, "Bin Pareto");	
//				System.out.println(binpareto.print(10));
				binpareto = binpareto.where(binpareto.stringColumn("bin").isEqualTo(".."));


				//
				//
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		if(! binpareto.isEmpty())
			dieyield = binpareto.getString(0, 1);

		return(dieyield);
	}

*/

	
	
	
	public String get_die_yield(Connection con, String step, String dev,  String lwr) {

		String dieyield = "-1";
		Table binpareto = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();

				String[] mylwr = lwr.split("-");


				/*
				String SQL_stmt = " select  " +
						" CONCAT (FldFailBin,FldFailError) as bin, " +
							" COUNT(CONCAT (FldFailBin,FldFailError)) as bincount " +
							" from  DbaLfoundryProbe.dbo.TblDie " +
							" where " +
//							" CONCAT(FldLot,FldWafer,FldRun) IN " + sql_lots + 
							" CONCAT(FldLot,FldWafer,FldRun) = '"+  lwr +"'" + 
//							" and FldRun = 1" +
							" and FldStep in ("+ step +")" +
							" GROUP BY CONCAT (FldFailBin,FldFailError)"  
							;
				 */


				String SQL_stmt = " select  " +
						" CONCAT (FldFailBin,FldFailError) as bin, " +
						" COUNT(CONCAT (FldFailBin,FldFailError)) as bincount " +
						" from  DbaLfoundryProbe.dbo.TblDie " +
						" where " +
						//							" CONCAT(FldLot,FldWafer,FldRun) IN " + sql_lots + 
						//							" CONCAT(FldLot,FldWafer,FldRun) = '"+  lwr +"'" + 
						" FldLot = " + "'"+  mylwr[0] +"'" +
						" and FldWafer = " + "'"+  mylwr[1] +"'" +
						" and FldRun = " + "'"+  mylwr[2] +"'" +
						//							" and FldRun = 1" +
						" and FldStep in ("+ step +")" +
						" GROUP BY CONCAT (FldFailBin,FldFailError)"  
						;



//				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				binpareto = Table.read().db(rs, "Bin Pareto");	
//				System.out.println(binpareto.print(10));
//				binpareto = binpareto.where(binpareto.stringColumn("bin").isEqualTo(".."));


				//
				//
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		if(! binpareto.isEmpty())
			dieyield = binpareto.getString(0, 1);

		return(dieyield);
	}

	
	
	
	
	
	
	
	



	public Table get_die_count(Connection con, String step, String dev,  String lwr) {

		Table binpareto = null; 

		try{

			if(con!=null){
				Statement stmt = con.createStatement();


				String SQL_stmt = " select  " +
						" COUNT(FldFailBin) as totdpw " +
						" from  DbaLfoundryProbe.dbo.TblDie " +
						" where " +
						//							" CONCAT(FldLot,FldWafer,FldRun) IN " + sql_lots + 

							" FldStep in ("+ step +")" +  
							" and CONCAT(FldLot,FldWafer,FldRun) = '"+  lwr +"'" 
							//							" and FldRun = 1" +
							;



				System.out.println(SQL_stmt);	

				ResultSet rs = stmt.executeQuery(SQL_stmt);
				binpareto = Table.read().db(rs, "Bin Pareto");	
				System.out.println(binpareto.print(10));
				//
				//
				rs.close();
				rs = null;

			}else System.out.println("Error: No active Connection");
		}catch(Exception e){
			e.printStackTrace();
		}

		return(binpareto);
	}









}

