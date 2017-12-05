package com.geostax.cassandra;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.geometry.jts.JTSFactoryFinder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.geostax.cassandra.index.S2Index;
import com.google.common.geometry.S2CellId;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;

public class tdraverquery {
	private S2Index s2index;
	private Cluster cluster = null;
	private Session session = null;
	public tdraverquery(){
		cluster = Cluster.builder().addContactPoint("192.168.210.110").build();
		s2index = new S2Index();
	}
	
	/*//查询某区域某一时刻通过的车辆(返回车辆编号和经纬度)
	public Map<String,Geometry> query1(String time,Geometry geom) throws Exception{
		long t=System.currentTimeMillis();
		
		Map<String,Geometry> tripid=new HashMap<String,Geometry>();;
		List<String> cell_ids=index.query1(10, geom);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH");
		Date date=sdf.parse(time);
		String qtime=sdf1.format(date);
		
		for(String cell_id:cell_ids){
			Geometry geometry=null;
			ByteBuffer buffer;
			WKBReader reader = new WKBReader();
			ResultSet rs = session.execute(QueryBuilder.select("cell_id","date","timestamp","trip_id","the_geom","lat","lng")
					.from("s2polygon10", "t_draver_ceshi")
					.where(QueryBuilder.eq("cell_id",cell_id ))
					.and(QueryBuilder.eq("date",qtime ))
					.and(QueryBuilder.eq("timestamp", time)));
			for(Row row:rs){
				String trip_id=row.getString("trip_id");
				buffer = row.getBytes("the_geom");
				geometry = reader.read(buffer.array()); 
				tripid.put(trip_id, geometry);
			}
		}
		
		session.close();
		cluster.close();
		System.out.println("经过的车辆数："+tripid.size());
		System.out.println("time:"+(System.currentTimeMillis()-t));
		return tripid;
	}*/
	
	
	//查询某区域某时段通过的车辆
	public void query2(String starttime,String endtime,Geometry geom) throws Exception{
		Session session = cluster.connect();
		long t=System.currentTimeMillis();
		Set<Integer> tripid=new HashSet<>();
		int count=0;
		
		List<String> timeprocess=timeprocess(starttime,endtime);
		List<String> betweendate=getBetweenDates(timeprocess.get(0),timeprocess.get(1));
		ResultSet rs=null;
		String select=null;
		List<S2CellId> cell_ids = s2index.index(10, geom);
		
		for(S2CellId cell_id:cell_ids){
			Geometry geometry=null;
			ByteBuffer buffer;
			WKBReader reader = new WKBReader();
			if(timeprocess.get(0).equals(timeprocess.get(1))){//同一小时的某个时段
				select="SELECT * FROM \"T_Drive\".track WHERE block='"+cell_id.toToken()+"' AND epoch='"+timeprocess.get(0)+"' AND timestamp>='"+timeprocess.get(2)+"' AND timestamp<='"+timeprocess.get(3)+"'";
				rs=session.execute(select);
				for(Row row:rs){
					int trip_id=row.getInt("id");
					//buffer = row.getBytes("the_geom");
					//String timestamp=row.getString("timestamp");
					//geometry = reader.read(buffer.array()); 
					tripid.add(trip_id);
					count++;
				}
				
			}else{//不同的h
				select="SELECT * FROM \"T_Drive\".track WHERE block='"+cell_id.toToken()+"' AND epoch='"+timeprocess.get(0)+"' AND timestamp>='"+timeprocess.get(2)+"'";
				rs=session.execute(select);
				for(Row row:rs){
					int trip_id=row.getInt("id");
					tripid.add(trip_id);
					count++;
					
				}
				
				for(String qdate:betweendate){
					select="SELECT * FROM \"T_Drive\".track WHERE block='"+cell_id.toToken()+"' AND epoch='"+qdate+"'";
					rs=session.execute(select);
					for(Row row:rs){
						int trip_id=row.getInt("id");
						tripid.add(trip_id);
						count++;
					}
					
				}
				select="SELECT * FROM \"T_Drive\".track WHERE block='"+cell_id.toToken()+"' AND epoch='"+timeprocess.get(1)+"' AND timestamp<='"+timeprocess.get(3)+"'";
				rs=session.execute(select);
				for(Row row:rs){
					int trip_id=row.getInt("id");
					tripid.add(trip_id);
					count++;
				}
				
			}
			
		}
		session.close();
		cluster.close();
		System.out.println("经过的车辆数："+tripid.size());
		System.out.println("轨迹点总计："+count);
		System.out.println("time:"+(System.currentTimeMillis()-t));
		
	}
	
	/*//查询某时段通过某路段的轨迹（轨迹：（p1,t1）,(p2,t2),......,(pn,tn) 有序 ）
	@SuppressWarnings({ "unused" })
	public void query3(String starttime,String endtime,Geometry geom) throws Exception{
		long t=System.currentTimeMillis();
		List<S2CellId> cell_ids=index.index1(10, geom);
        List<String> timeprocess=timeprocess(starttime,endtime);
		List<String> betweendate=getBetweenDates(timeprocess.get(0),timeprocess.get(1));
		
		Set<String> tripid=new HashSet<>();
		int count=0;
		
		ResultSet rs=null;
		String select=null;
		
		System.out.println("cell个数"+cell_ids.size());
		for(S2CellId cell_id:cell_ids){
			Geometry geometry=null;
			ByteBuffer buffer;
			WKBReader reader = new WKBReader();
			
			if(timeprocess.get(0).equals(timeprocess.get(1))){//同一个h
				select="SELECT * FROM t_draver_ceshi1 WHERE qcell_id='"+cell_id.toToken()+"' AND date='"+timeprocess.get(0)+"' AND timestamp>='"+timeprocess.get(2)+"' AND timestamp<='"+timeprocess.get(3)+"'";
				rs=session.execute(select);
				for(Row row:rs){
					String trip_id=row.getString("trip_id");
					buffer = row.getBytes("the_geom");
					String timestamp=row.getString("timestamp");
					geometry = reader.read(buffer.array()); 
					count++;
					tripid.add(trip_id);
				}
				
			}else{//不同h
				select="SELECT * FROM t_draver_ceshi1 WHERE qcell_id='"+cell_id.toToken()+"' AND date='"+timeprocess.get(0)+"' AND timestamp>='"+timeprocess.get(2)+"'";
				rs=session.execute(select);
				for(Row row:rs){
					String trip_id=row.getString("trip_id");
					tripid.add(trip_id);
					count++;
				}
				for(String qdate:betweendate){
					select="SELECT * FROM t_draver_ceshi1 WHERE qcell_id='"+cell_id.toToken()+"' AND date='"+qdate+"'";
					rs=session.execute(select);
					for(Row row:rs){
						String trip_id=row.getString("trip_id");
						tripid.add(trip_id);
						count++;
					}
				}
				select="SELECT * FROM t_draver_ceshi1 WHERE qcell_id='"+cell_id.toToken()+"' AND date='"+timeprocess.get(1)+"' AND timestamp<='"+timeprocess.get(3)+"'";
				rs=session.execute(select);
				for(Row row:rs){
					String trip_id=row.getString("trip_id");
					tripid.add(trip_id);
					count++;
				}
			}
			
		}
		session.close();
		cluster.close();
		
		System.out.println("经过的车辆数："+tripid.size());
		System.out.println("轨迹点总计："+count);
		System.out.println("time:"+(System.currentTimeMillis()-t));
	}
	
	//查询某一时间范围车辆t的轨迹（轨迹：（p1,t1）,(p2,t2),......,(pn,tn) 有序）
	public List<String> query4(String starttime,String endtime,String trip) throws Exception{
		long t=System.currentTimeMillis();
		List<String>trajectory=new ArrayList<>();
		
		ResultSet selectresult = null;
		String select=null;
		Geometry geometry=null;
		ByteBuffer buffer;
		WKBReader reader = new WKBReader();
		int count=0;
		
		List<String> timeprocess=timeprocess(starttime,endtime);
		List<String> betweendate=getBetweenDates(timeprocess.get(0),timeprocess.get(1));
		
		if(timeprocess.get(0).equals(timeprocess.get(1))){
			//System.out.println(1);
			select="SELECT * FROM trajectory1 WHERE trip_id='"+trip+"' AND date='"+timeprocess.get(0)+"' AND timestamp>='"+timeprocess.get(2)+"' AND timestamp<='"+timeprocess.get(3)+"'";	
			selectresult=session.execute(select);	
			for(Row row:selectresult){
				buffer = row.getBytes("the_geom");
				String timestamp=row.getString("timestamp");
				geometry = reader.read(buffer.array()); 
				trajectory.add(timestamp+","+geometry.getCoordinate().x+" "+geometry.getCoordinate().y);
				count++;
			}
			
		}else{
			select="SELECT * FROM trajectory1 WHERE trip_id='"+trip+"' AND date='"+timeprocess.get(0)+"' AND timestamp>='"+timeprocess.get(2)+"'";	
			selectresult=session.execute(select);
			for(Row row:selectresult){
				buffer = row.getBytes("the_geom");
				String timestamp=row.getString("timestamp");
				geometry = reader.read(buffer.array()); 
				trajectory.add(timestamp+","+geometry.getCoordinate().x+" "+geometry.getCoordinate().y);
				count++;
			}
			
			for(String qdate:betweendate){
				select="SELECT * FROM trajectory1 WHERE trip_id='"+trip+"' AND date='"+qdate+"'";	
				selectresult=session.execute(select);
				selectresult=session.execute(select);	
				for(Row row:selectresult){
					buffer = row.getBytes("the_geom");
					String timestamp=row.getString("timestamp");
					geometry = reader.read(buffer.array()); 
					trajectory.add(timestamp+","+geometry.getCoordinate().x+" "+geometry.getCoordinate().y);
					count++;
				}
			}
			select="SELECT * FROM trajectory1 WHERE trip_id='"+trip+"' AND date='"+timeprocess.get(1)+"' AND timestamp<='"+timeprocess.get(3)+"'";	
			selectresult=session.execute(select);	
			for(Row row:selectresult){
				buffer = row.getBytes("the_geom");
				String timestamp=row.getString("timestamp");
				geometry = reader.read(buffer.array()); 
				trajectory.add(timestamp+","+geometry.getCoordinate().x+" "+geometry.getCoordinate().y);
				count++;
			}
		}
		session.close();
		cluster.close();
		System.out.println("轨迹点总计："+count);
		System.out.println("轨迹："+trajectory);
		System.out.println("time:"+(System.currentTimeMillis()-t));
		return trajectory;
		
		
	}
	
	//查询车+空间
	public List<String> query5(String tripid,Geometry geom) throws Exception{
		long t=System.currentTimeMillis();
		List<String> cell_ids=index.query1(10, geom);
		ResultSet rs=null;
		String select=null;
		List<String>trajectory=new ArrayList<>();
	    int count=0;
		for(String cell_id:cell_ids){
			Geometry geometry=null;
			ByteBuffer buffer;
			WKBReader reader = new WKBReader();
			select="SELECT * FROM trajectory2 WHERE trip_id='"+tripid+"' AND qcell_id='"+cell_id+"'";
			rs=session.execute(select);	
			for(Row row:rs){
				buffer = row.getBytes("the_geom");
				String timestamp=row.getString("timestamp");
				geometry = reader.read(buffer.array()); 
				trajectory.add(timestamp+","+geometry.getCoordinate().x+" "+geometry.getCoordinate().y);
				count++;
			}
			
		}
		session.close();
		cluster.close();
		System.out.println("轨迹点总计："+count);
		//System.out.println("轨迹："+trajectory);
		System.out.println("time:"+(System.currentTimeMillis()-t));
		return trajectory;
		
		
	}*/
	
	//获取两个时间之间的时间（不包含起止时间）
	private List<String> getBetweenDates(String starttime, String endtime) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH"); 
		Date start=sdf.parse(starttime);
		Date end=sdf.parse(endtime);
	    List<String> result = new ArrayList<>();
	    //设置calendar的时间
	    Calendar tempStart = Calendar.getInstance();
	    tempStart.setTime(start);
	    tempStart.add(Calendar.HOUR_OF_DAY, 1);
	    
	    Calendar tempEnd = Calendar.getInstance();
	    tempEnd.setTime(end);
	    //测试起始日期在终止日期之前
	    while (tempStart.before(tempEnd)) {
	        result.add(sdf.format(tempStart.getTime()));
	        //根据日历规则，为给定的日历字段添加或减去指定的时间量  
	        tempStart.add(Calendar.HOUR_OF_DAY, 1);
	    }
	    //System.out.println(result);
	    return result;
	}
	
	//为查询4创建MATERIALIZED VIEW
	public void createmv1(){
		String createmv="CREATE MATERIALIZED VIEW trajectory1 AS SELECT * FROM t_draver_ceshi1 WHERE trip_id IS NOT NULL AND date IS NOT NULL AND timestamp IS NOT NULL and qcell_id IS NOT NULL PRIMARY KEY((trip_id,date),timestamp,qcell_id) WITH CLUSTERING ORDER BY(timestamp ASC)";
		session.execute(createmv);
		session.close();
		cluster.close();
				
	}
	
	//MATERIALIZED VIEW对应查询5  车辆+空间查询
	public void createmv2(){
		String createmv="CREATE MATERIALIZED VIEW trajectory2 AS SELECT * FROM t_draver_ceshi1 WHERE trip_id IS NOT NULL AND date IS NOT NULL AND timestamp IS NOT NULL and qcell_id IS NOT NULL PRIMARY KEY((trip_id),qcell_id,date,timestamp) WITH CLUSTERING ORDER BY(timestamp ASC)";
		session.execute(createmv);
		session.close();
		cluster.close();
				
	}
	
	public void dropmv(){
		String dropmv="DROP MATERIALIZED VIEW trajectory1";
		session.execute(dropmv);
		session.close();
		cluster.close();
				
	}
	
	
	//时间处理，返回起止日期和起止时刻
	public List<String> timeprocess(String starttime,String endtime) throws Exception{
		List<String> timeprocess=new ArrayList<>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH");
		Date dbegin = sdf.parse(starttime);  
		Date dend = sdf.parse(endtime); 
		String dBegin=sdf1.format(dbegin);
		String dEnd=sdf1.format(dend);
		String tBegin = starttime;  
		String tEnd = endtime; 
		timeprocess.add(dBegin);
		timeprocess.add(dEnd);
		timeprocess.add(tBegin);
		timeprocess.add(tEnd);
		//System.out.println(timeprocess);
		return timeprocess;
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
		//new tdraverquery().createmv1();
		//new tdraverquery().dropmv();
		
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory( null ); 
		String starttime="2008-02-02 15:46:08";
		String endtime="2008-02-02 23:40:32";
		
		/*
		//query1
		String time="2008-02-04 23:38:37";
		Coordinate[] coordinates=new Coordinate[]{new Coordinate(116.416,39.929),new Coordinate(116.475,39.929),new Coordinate(116.475,39.996),new Coordinate(116.416,39.996),new Coordinate(116.416,39.929)};
		Geometry geom=geometryFactory.createPolygon(coordinates);
		//new tdraverquery().query1(time,geom);*/
		
		//query2
		Coordinate[] coordinates=new Coordinate[]{new Coordinate(116.416,39.929),new Coordinate(116.475,39.929),new Coordinate(116.475,39.996),new Coordinate(116.416,39.996),new Coordinate(116.416,39.929)};
		Geometry geom= geometryFactory.createMultiPolygon(new Polygon[]{geometryFactory.createPolygon(coordinates)});
		new tdraverquery().query2(starttime,endtime,geom);
		
		/*//query3
		Coordinate[] coordinates1=new Coordinate[]{new Coordinate(116.344,39.896),new Coordinate(116.446,39.902),new Coordinate(116.526,39.913),new Coordinate(116.55,39.984),new Coordinate(116.653,39.964)};
		Geometry geom1=geometryFactory.createLineString(coordinates1);
		new tdraverquery().query3(starttime,endtime,geom1);
		
		//query4
		String trip="1";
		new tdraverquery().query4(starttime, endtime, trip);
		
		//query5
		String trip="1277";
		Coordinate[] coordinates=new Coordinate[]{new Coordinate(116.416,39.929),new Coordinate(116.475,39.929),new Coordinate(116.475,39.996),new Coordinate(116.416,39.996),new Coordinate(116.416,39.929)};
		Geometry geom=geometryFactory.createPolygon(coordinates);
		new tdraverquery().query5(trip,geom);*/
		
		
	}

}
