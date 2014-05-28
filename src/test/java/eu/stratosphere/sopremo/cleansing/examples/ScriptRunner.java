package eu.stratosphere.sopremo.cleansing.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.QueryParser;
import eu.stratosphere.sopremo.client.DefaultClient;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.query.QueryParserException;
import eu.stratosphere.sopremo.server.SopremoTestServer;

public class ScriptRunner{
	protected SopremoTestServer testServer;

	protected DefaultClient client;

	protected File inputDir;
	
	@Test
	public void runScript() throws IOException {
		File scriptFile1 = new File("src/main/meteor/usCongress.script");
		File scriptFile2 = new File("src/main/meteor/Freebase-Persons.script");
		File scriptFile3 = new File("src/main/meteor/Freebase-Politicians.script");
		File scriptFile4 = new File("src/main/meteor/record_linkage_persons.script");
		File scriptFile5 = new File("src/main/meteor/Freebase-Company.script");
		File scriptFile6 = new File("src/main/meteor/fusion_persons.script");
		File scriptFile7 = new File("src/main/meteor/combined.script");
		File scriptFile8 = new File("src/main/meteor/subScripts/usearmarksPersonsSub.script");
		File scriptFile9 = new File("src/main/meteor/subScripts/uscongressPoliticiansSub.script");
		File scriptFile10 = new File("src/main/meteor/subScripts/usearmarksSub.script");
		File scriptFile11 = new File("src/main/meteor/subScripts/freebasePoliticiansSub.script");
		File scriptFile12 = new File("src/main/meteor/usSpending.script");
		File scriptFile13 = new File("src/main/meteor/combinedPersons.script");
		File scriptFile14 = new File("src/main/meteor/bugTC.script");
		File scriptFile15 = new File("src/main/meteor/usCongressNew.script");
		File scriptFile16 = new File("src/main/meteor/finalScripts/freebaseNew.script");
		final SopremoPlan plan = parseScript(scriptFile16);
		
		this.client.submit(plan, null, true);
	}

	@Before
	public final void setup() throws Exception {
		this.testServer = new SopremoTestServer(true);
		this.inputDir = this.testServer.createDir("input");

		this.client = new DefaultClient();
		this.client.setServerAddress(this.testServer.getServerAddress());
		this.client.setUpdateTime(100);
	}

	@After
	public void teardown() throws Exception {
		this.client.close();
		this.testServer.close();
	}
	
	public SopremoPlan parseScript(final File script) {
		SopremoPlan plan = null;
		try {
			final QueryParser queryParser = new QueryParser().withInputDirectory(script.getParentFile());
			
			plan = queryParser.tryParse(this.loadScriptFromFile(script));
		} catch (final QueryParserException e) {
			final AssertionError error =
				new AssertionError(String.format("could not parse script: %s", e.getMessage()));
			error.initCause(e);
			throw error;
		}

		Assert.assertNotNull("could not parse script", plan);

		return plan;
	}
	
	private String loadScriptFromFile(final File scriptFile) {
		try {
			final BufferedReader reader = new BufferedReader(new FileReader(scriptFile));
			final StringBuilder builder = new StringBuilder();
			int ch;
			while ((ch = reader.read()) != -1)
				builder.append((char) ch);
			reader.close();
			return builder.toString();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}

	}
}
