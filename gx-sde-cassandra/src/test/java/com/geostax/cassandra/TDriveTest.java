package com.geostax.cassandra;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.geostax.cassandra.index.S2Index;
import com.google.common.geometry.S2CellId;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBReader;

public class TDriveTest {

	private Cluster cluster = null;
	private S2Index s2index;
	private DateFormat df1;
	private DateFormat df2;

	public TDriveTest() {
		cluster = Cluster.builder().addContactPoint("192.168.210.110").build();
		s2index = new S2Index();
		df1 = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
		df2 = new SimpleDateFormat("yyyyMMddHH");
	}

	public void ingest(String dic) throws Exception {

		File dir = new File(dic);
		File[] list = dir.listFiles();
		BatchStatement bs = new BatchStatement();
		Session session = cluster.connect();

		for (File file : list) {
			System.out.println(file.getName());
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String item[] = line.split(",");
				int id = Integer.parseInt(item[0]);
				String stime = item[1];
				double lon = Double.parseDouble(item[2]);
				double lat = Double.parseDouble(item[3]);
				Date date = df1.parse(stime);
				String epoch = df2.format(date);
				S2CellId cell = s2index.index(lat, lon);
				String cell_id = cell.toToken();
				String block = cell.parent(12).toToken();

				SimpleStatement stat = new SimpleStatement(
						"INSERT INTO \"T_Drive\".track_l12 (block,epoch,timestamp,id,cell,lat,lng) VALUES (?,?,?,?,?,?,?);",
						block, epoch, date, id, cell_id, lat, lon);
				bs.add(stat);
				if (bs.size() == 100) {
					session.execute(bs);
					bs.clear();
				}
				// System.out.println(epoch+","+id+","+stime+","+lon+","+lat);
			}
			scanner.close();
		}

		cluster.close();

	}

	public void query() {
		Session session = cluster.connect();
		SimpleStatement stat = new SimpleStatement("SELECT * FROM \"T_Drive\".track where block=? and date=?", "35f04f",
				"2008020406");
		ResultSet rs = session.execute(stat);
		for (Row row : rs) {
			System.out.println(row);
		}

	}

	public void queryByGeomAndTime(Geometry geom, String date1, String date2) throws Exception {
		Session session = cluster.connect();
		List<Row> result = new ArrayList<>();
		Set<Integer> tripid = new TreeSet<>();
		int count = 0;
		long t0 = System.currentTimeMillis();
		List<String> range = timeprocess2(date1, date2);
		List<String> epoch = getBetweenDates2(range.get(0), range.get(1));
		// System.out.println(epoch);
		List<S2CellId> cell_ids = s2index.index(12, geom);
	    System.out.println(cell_ids);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date start = sdf.parse(date1);
		Date end = sdf.parse(date2);
		for (S2CellId cell_id : cell_ids) {
			// System.out.println(cell_id.toToken());
			for (String stime : epoch) {
				// System.out.println(stime);
				SimpleStatement stat = new SimpleStatement(
						"SELECT * FROM \"T_Drive\".track_l12 where block=? and epoch = ? and timestamp>=? and timestamp<?;",
						cell_id.toToken(), stime, start, end);
			
				ResultSet rs = session.execute(stat);
				for (Row row : rs) {
					int trip_id = row.getInt("id");
					// String timestamp = row.getString("timestamp");
					/*
					 * try { if (!com.vividsolutions.jts.geom.prep.
					 * PreparedGeometryFactory.prepare(geometry).intersects(geom
					 * )) { continue; } } catch (Exception ex) {
					 * ex.printStackTrace(); }
					 */
					tripid.add(trip_id);
					result.add(row);
					count++;
				}
			}
		}

		System.out.println(System.currentTimeMillis() - t0 + " ms");
		System.out.println("经过的车辆数：" + tripid.size());
		System.out.println(count);
		session.close();
		cluster.close();

	}

	// 查询某区域某一时刻通过的车辆(返回车辆编号和经纬度)
	public Map<String, Geometry> query1(String time, Geometry geom) throws Exception {
		Session session = cluster.connect();
		Map<String, Geometry> tripid = new HashMap<String, Geometry>();
		;
		List<S2CellId> cell_ids = s2index.index(10, geom);
		String[] sptime = time.split(" ");
		String qtime = sptime[0];
		for (S2CellId cell_id : cell_ids) {
			Geometry geometry = null;
			ByteBuffer buffer;
			WKBReader reader = new WKBReader();
			ResultSet rs = session
					.execute(QueryBuilder.select("cell_id", "date", "timestamp", "trip_id", "the_geom", "lat", "lng")
							.from("s2polygon10", "t_draver_ceshi").where(QueryBuilder.eq("cell_id", cell_id))
							.and(QueryBuilder.eq("date", qtime)).and(QueryBuilder.eq("timestamp", sptime[1])));
			for (Row row : rs) {
				String trip_id = row.getString("trip_id");
				buffer = row.getBytes("the_geom");
				geometry = reader.read(buffer.array());
				// 轨迹处理后再做这一步
				/*
				 * try { if
				 * (!com.vividsolutions.jts.geom.prep.PreparedGeometryFactory.
				 * prepare(geometry).intersects(geom)) { continue; } } catch
				 * (Exception ex) { ex.printStackTrace(); }
				 */
				// System.out.println(trip_id);
				tripid.put(trip_id, geometry);
			}
		}

		session.close();
		cluster.close();
		System.out.println("经过的车辆数：" + tripid.size());
		return tripid;
	}

	public void query2(String starttime, String endtime, Geometry geom) throws Exception {
		Session session = cluster.connect();

		long t0 = System.currentTimeMillis();
		List<Row> result = new ArrayList<>();
		Set<String> tripid = new HashSet<>();
		int count = 0;

		List<String> timeprocess = timeprocess2(starttime, endtime);

		List<String> betweendate = getBetweenDates2(timeprocess.get(0), timeprocess.get(1));
		List<ResultSet> rslist = new ArrayList<ResultSet>();
		ResultSet rs = null;
		String select = null;
		List<S2CellId> cell_ids = s2index.index(10, geom);

		for (S2CellId cell_id : cell_ids) {

			Geometry geometry = null;
			ByteBuffer buffer;
			WKBReader reader = new WKBReader();

			select = "SELECT * FROM \"T_Drive\".track WHERE block=? AND epoch IN ? AND timestamp>=? AND timestamp<=?;";
			SimpleStatement stat = new SimpleStatement(select, cell_id.toToken(), timeprocess, timeprocess.get(2),
					timeprocess.get(3));
			rs = session.execute(stat);

			/*
			 * if (timeprocess.get(0).equals(timeprocess.get(1))) {// 同一天的某个时段
			 * select = "SELECT * FROM \"T_Drive\".track WHERE block='" +
			 * cell_id.toToken() + "' AND date='" + timeprocess.get(0) +
			 * "' AND timestamp>='" + timeprocess.get(2) + "' AND timestamp<='"
			 * + timeprocess.get(3) + "'"; rs = session.execute(select);
			 * rslist.add(rs); } else {// 不同的日期 select =
			 * "SELECT * FROM t_draver_ceshi WHERE qcell_id='" + cell_id +
			 * "' AND date='" + timeprocess.get(0) + "' AND timestamp>='" +
			 * timeprocess.get(2) + "'"; rs = session.execute(select);
			 * rslist.add(rs); for (String qdate : betweendate) { select =
			 * "SELECT * FROM t_draver_ceshi WHERE qcell_id='" + cell_id +
			 * "' AND date='" + qdate + "'"; rs = session.execute(select);
			 * rslist.add(rs); } select =
			 * "SELECT * FROM t_draver_ceshi WHERE qcell_id='" + cell_id +
			 * "' AND date='" + timeprocess.get(1) + "' AND timestamp<='" +
			 * timeprocess.get(3) + "'"; rs = session.execute(select);
			 * rslist.add(rs); }
			 */

			for (Row row : rs) {
				String trip_id = row.getString("id");
				String timestamp = row.getString("timestamp");
				/*
				 * try { if (!com.vividsolutions.jts.geom.prep.
				 * PreparedGeometryFactory.prepare(geometry).intersects(geom ))
				 * { continue; } } catch (Exception ex) { ex.printStackTrace();
				 * }
				 */
				tripid.add(trip_id);
				result.add(row);
				count++;
				// System.out.println(trip_id);
			}
		}

		System.out.println(System.currentTimeMillis() - t0 + " ms");
		System.out.println("经过的车辆数：" + tripid.size());
		System.out.println(count);
		session.close();
		cluster.close();
	}

	// 查询某时段通过某路段的轨迹（轨迹：（p1,t1）,(p2,t2),......,(pn,tn) 有序 ）
	@SuppressWarnings({ "unused" })
	public void query3(String starttime, String endtime, Geometry geom) throws Exception {
		Session session = cluster.connect();
		List<S2CellId> cell_ids = s2index.index(10, geom);
		List<String> timeprocess = timeprocess(starttime, endtime);
		List<String> betweendate = getBetweenDates(timeprocess.get(0), timeprocess.get(1));
		List<Row> result = new ArrayList<>();
		Set<String> tripid = new HashSet<>();
		int count = 0;
		ResultSet rs = null;
		String select = null;
		List<ResultSet> rslist = new ArrayList<ResultSet>();

		for (S2CellId cell_id : cell_ids) {
			Geometry geometry = null;
			ByteBuffer buffer;
			WKBReader reader = new WKBReader();
			if (timeprocess.get(0).equals(timeprocess.get(1))) {// 同一天的某个时段
				select = "SELECT * FROM t_draver_ceshi WHERE qcell_id='" + cell_id + "' AND date='" + timeprocess.get(0)
						+ "' AND timestamp>='" + timeprocess.get(2) + "' AND timestamp<='" + timeprocess.get(3) + "'";
				rs = session.execute(select);
				rslist.add(rs);
			} else {// 不同的日期
				select = "SELECT * FROM t_draver_ceshi WHERE qcell_id='" + cell_id + "' AND date='" + timeprocess.get(0)
						+ "' AND timestamp>='" + timeprocess.get(2) + "'";
				rs = session.execute(select);
				rslist.add(rs);
				for (String qdate : betweendate) {
					select = "SELECT * FROM t_draver_ceshi WHERE qcell_id='" + cell_id + "' AND date='" + qdate + "'";
					rs = session.execute(select);
					rslist.add(rs);
				}
				select = "SELECT * FROM t_draver_ceshi WHERE qcell_id='" + cell_id + "' AND date='" + timeprocess.get(1)
						+ "' AND timestamp<='" + timeprocess.get(3) + "'";
				rs = session.execute(select);
				rslist.add(rs);
			}
			for (ResultSet res : rslist) {
				for (Row row : res) {
					String trip_id = row.getString("trip_id");
					buffer = row.getBytes("the_geom");
					String timestamp = row.getString("timestamp");
					geometry = reader.read(buffer.array());
					/*
					 * try { if (!com.vividsolutions.jts.geom.prep.
					 * PreparedGeometryFactory.prepare(geometry).intersects(geom
					 * )) { continue; } } catch (Exception ex) {
					 * ex.printStackTrace(); }
					 */
					tripid.add(trip_id);
					result.add(row);
					count++;
					// System.out.println(trip_id);
				}
			}
			rslist.clear();

		}
		session.close();
		cluster.close();
	}

	// 查询某一时间范围车辆t的轨迹（轨迹：（p1,t1）,(p2,t2),......,(pn,tn) 有序）
	public List<Map<String, Geometry>> query4(String starttime, String endtime, String trip) throws Exception {
		Session session = cluster.connect();
		List<ResultSet> rslist = new ArrayList<ResultSet>();
		List<Map<String, Geometry>> trajectory = new ArrayList<>();
		Map<String, Geometry> trajectorypoint = new HashMap<>();
		ResultSet selectresult = null;
		String select = null;

		List<String> timeprocess = timeprocess(starttime, endtime);

		Geometry geometry = null;
		ByteBuffer buffer;
		WKBReader reader = new WKBReader();

		List<String> betweendate = getBetweenDates(timeprocess.get(0), timeprocess.get(1));
		if (timeprocess.get(0).equals(timeprocess.get(1))) {
			// System.out.println(1);
			select = "SELECT * FROM trajectory1 WHERE trip_id='" + trip + "' AND date='" + timeprocess.get(0)
					+ "' AND timestamp>='" + timeprocess.get(2) + "' AND timestamp<='" + timeprocess.get(3) + "'";
			selectresult = session.execute(select);
			rslist.add(selectresult);
		} else {
			select = "SELECT * FROM trajectory1 WHERE trip_id='" + trip + "' AND date='" + timeprocess.get(0)
					+ "' AND timestamp>='" + timeprocess.get(2) + "'";
			selectresult = session.execute(select);
			rslist.add(selectresult);

			for (String qdate : betweendate) {
				select = "SELECT * FROM trajectory1 WHERE trip_id='" + trip + "' AND date='" + qdate + "'";
				selectresult = session.execute(select);
				rslist.add(selectresult);
			}
			select = "SELECT * FROM trajectory1 WHERE trip_id='" + trip + "' AND date='" + timeprocess.get(1)
					+ "' AND timestamp<='" + timeprocess.get(3) + "'";
			selectresult = session.execute(select);
			rslist.add(selectresult);

		}

		for (ResultSet res : rslist) {
			for (Row row : res) {
				buffer = row.getBytes("the_geom");
				String timestamp = row.getString("timestamp");
				String date = row.getString("date");
				String trajectorytime = "" + date + " " + timestamp + "";
				geometry = reader.read(buffer.array());
				trajectorypoint.put(trajectorytime, geometry);
				trajectory.add(trajectorypoint);
				System.out.println(trajectorytime);
			}
		}
		System.out.println(trajectory.size());
		session.close();
		cluster.close();
		return trajectory;

	}

	// 获取两个日期之间的日期（不包含起止日期）
	private List<String> getBetweenDates(String starttime, String endtime) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date start = sdf.parse(starttime);
		Date end = sdf.parse(endtime);
		List<String> result = new ArrayList<>();
		// 设置calendar的时间
		Calendar tempStart = Calendar.getInstance();
		tempStart.setTime(start);
		tempStart.add(Calendar.DAY_OF_YEAR, 1);

		Calendar tempEnd = Calendar.getInstance();
		tempEnd.setTime(end);
		// 测试起始日期在终止日期之前
		while (tempStart.before(tempEnd)) {
			result.add(sdf.format(tempStart.getTime()));
			// 根据日历规则，为给定的日历字段添加或减去指定的时间量
			tempStart.add(Calendar.DAY_OF_YEAR, 1);
		}
		// System.out.println(result);
		return result;
	}

	// 获取两个日期之间的日期（不包含起止日期）
	private List<String> getBetweenDates2(String starttime, String endtime) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		Date start = sdf.parse(starttime);
		Date end = sdf.parse(endtime);
		List<String> result = new ArrayList<>();
		// 设置calendar的时间
		Calendar tempStart = Calendar.getInstance();
		tempStart.setTime(start);
		tempStart.add(Calendar.HOUR_OF_DAY, 1);

		Calendar tempEnd = Calendar.getInstance();
		tempEnd.setTime(end);
		// 测试起始日期在终止日期之前
		while (tempStart.before(tempEnd)) {
			result.add(sdf.format(tempStart.getTime()));
			// 根据日历规则，为给定的日历字段添加或减去指定的时间量
			tempStart.add(Calendar.HOUR_OF_DAY, 1);
		}
		// System.out.println(result);
		return result;
	}

	// 时间处理，返回起止日期和起止时刻
	public List<String> timeprocess(String starttime, String endtime) throws Exception {
		List<String> timeprocess = new ArrayList<>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");
		Date begin = sdf.parse(starttime);
		Date end = sdf.parse(endtime);
		String dBegin = sdf1.format(begin);
		String dEnd = sdf1.format(end);
		String tBegin = sdf2.format(begin);
		String tEnd = sdf2.format(end);
		timeprocess.add(dBegin);
		timeprocess.add(dEnd);
		timeprocess.add(tBegin);
		timeprocess.add(tEnd);
		// System.out.println(timeprocess);
		return timeprocess;

	}

	public List<String> timeprocess2(String starttime, String endtime) throws Exception {
		List<String> timeprocess = new ArrayList<>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");
		Date begin = sdf.parse(starttime);
		Date end = sdf.parse(endtime);
		String dBegin = sdf1.format(begin);
		String dEnd = sdf1.format(end);
		String tBegin = sdf.format(begin);
		String tEnd = sdf.format(end);
		timeprocess.add(dBegin);
		timeprocess.add(dEnd);
		timeprocess.add(tBegin);
		timeprocess.add(tEnd);
		// System.out.println(timeprocess);
		return timeprocess;

	}

	public static void main(String[] args) throws Exception {
		// new TDriveTest().ingest("E:\\Data\\T-Drive\\T-Drive");
		// new TDriveTest().query();
		// System.out.println(new
		// TDriveTest().getBetweenDates2("2017120506","2017120602"));
		String starttime="2008-02-02 15:46:08";
		String endtime="2008-02-02 23:40:32";
		// String trip = "1";
		// new tdraverquery().createmv1();
		// new tdraverquery().query4(starttime, endtime, trip);
		// new tdraverquery().dropmv();
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
		Coordinate[] coordinates = new Coordinate[] { new Coordinate(116.416, 39.929), new Coordinate(116.475, 39.929),
				new Coordinate(116.475, 39.996), new Coordinate(116.416, 39.996), new Coordinate(116.416, 39.929) };
		Geometry geom = geometryFactory
				.createMultiPolygon(new Polygon[] { geometryFactory.createPolygon(coordinates) });
		 
		new TDriveTest().queryByGeomAndTime(geom, starttime, endtime);

	}
}
