/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.function.SopremoFunction0;
import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.function.SopremoFunction2;
import eu.stratosphere.sopremo.function.SopremoFunction3;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;

public class GovWildUDF implements BuiltinProvider {

	@Name(noun = "extract_name")
	public static class EXTRACT_NAME extends
			SopremoFunction2<TextNode, TextNode> {

		private transient DictionarySearch dict = new DictionarySearch();

		@Override
		protected IJsonNode call(TextNode valueNode, TextNode dictionary) {
			System.out.println(valueNode.toString());
			IArrayNode<IJsonNode> result = new ArrayNode<IJsonNode>(2);
			result.set(1, new ArrayNode<TextNode>());
			String value = valueNode.toString();

			String addition;
			while ((addition = this.dict.search(value, dictionary.toString(),
					0, true, true)) != null) {
				((IArrayNode<TextNode>) result.get(1)).add(TextNode
						.valueOf(addition.trim()));
				value = this.replaceAddition(value.trim(), addition.trim());
			}
			result.set(0, TextNode.valueOf(value.trim()));
			return result;
		}

		private String replaceAddition(String value, String addition) {
			String valueLC = " " + value.toLowerCase() + " ";
			String additionLC = " " + addition.toLowerCase() + " ";
			value = " " + value + " ";
			int start;
			while ((start = valueLC.indexOf(additionLC)) != -1) {
				if (start == 0)
					value = value.substring(additionLC.length() - 1);
				else {
					value = value.substring(0, start)
							+ value.substring(start + additionLC.length());
				}
				valueLC = " " + value.trim().toLowerCase() + " ";
			}
			return value.trim();
		}

		// @Override
		// protected IJsonNode call(TextNode valueNode, TextNode dictionary,
		// TextNode id) {
		// BooleanNode ignoreCase = BooleanNode.TRUE;
		// System.out.println(id);
		// IArrayNode<IJsonNode> result = new ArrayNode<IJsonNode>(2);
		// StringBuilder builder = new StringBuilder();
		// String value = valueNode.toString();
		//
		// value = extractSuffixes(dictionary, builder, value,
		// ignoreCase.getBooleanValue());
		//
		// String suffixes = builder.toString().replace(".", "").trim();
		// if (value.endsWith(",")) {
		// value = value.substring(0, value.length() - 1);
		// }
		//
		// result.set(0, TextNode.valueOf(value.trim()));
		// result.set(1, TextNode.valueOf(suffixes));
		//
		// return result;
		// }
		//
		// private String extractSuffixes(TextNode dictionary,
		// StringBuilder builder, String value, boolean ignoreCase) {
		// String suffix;
		// while ((suffix = this.dict.search(value, dictionary.toString(), 0,
		// ignoreCase, true)) != null) {
		// builder.append(", ").append(suffix);
		// value = this.replaceSuffix(value, suffix, ignoreCase);
		// // value = value.replaceFirst(suffix, "");
		// }
		// builder.delete(0, 2);
		// return value;
		// }
		//
		// private String replaceSuffix(String value, String suffix,
		// boolean ignoreCase) {
		// int startIndex;
		// int endIndex;
		// String formattedValue = this.format(value);
		// if (ignoreCase) {
		// startIndex = formattedValue.toLowerCase().indexOf(
		// this.format(suffix).toLowerCase());
		// } else {
		// startIndex = formattedValue.indexOf(this.format(suffix));
		// }
		// endIndex = startIndex + suffix.length();
		// String deleteString = formattedValue
		// .substring(startIndex, endIndex);
		// return value.replace(deleteString, "").trim();
		// }
		//
		// private String format(String value) {
		// return " " + value.trim() + " ";
		// }
	};

	@Name(noun = "array_get")
	public static class ARRAY_GET extends
			SopremoFunction2<IArrayNode<IJsonNode>, IntNode> {

		@Override
		protected IJsonNode call(IArrayNode<IJsonNode> array, IntNode index) {
			return array.get(index.getIntValue());
		}
	}

	@Name(noun = "getValue")
	public static class GET_VALUE extends
			SopremoFunction2<IObjectNode, TextNode> {

		@Override
		protected IJsonNode call(IObjectNode object, TextNode field) {
			return object.get(field.toString());
		}

	}

	@Name(noun = "normalize_name")
	public static class NORMALIZE_NAME extends
			SopremoFunction2<TextNode, TextNode> {

		private DictionarySearch dict = new DictionarySearch();

		@Override
		protected IJsonNode call(TextNode fullName, TextNode officialTitlesPath) {
			if (fullName == null)
				return NullNode.getInstance();

			String[] nameAndAddition = this.splitName(fullName);

			String addition = "";
			StringBuilder nameAdd = new StringBuilder("");
			while (addition != null) {
				addition = this.dict.search(nameAndAddition[0],
						officialTitlesPath.toString(), 0, false, false);
				if (addition != null) {
					nameAdd.append(" ").append(addition);
					nameAndAddition[0] = nameAndAddition[0].replace(addition,
							"");
				}
			}

			String name = nameAndAddition[0];

			if (nameAndAddition.length > 1) {
				nameAdd.append(" ").append(nameAndAddition[1].trim());
			}

			IArrayNode<IJsonNode> result = this.normalizeName(name);

			result.add(TextNode.valueOf(nameAdd.toString().trim()));

			return result;
		}

		private String[] splitName(TextNode fullName) {
			String[] nameAndAddition = fullName.toString().split(",");

			if (nameAndAddition.length > 2) {
				for (int i = 2; i < nameAndAddition.length; i++) {
					nameAndAddition[1] += " " + nameAndAddition[i];
				}
				return Arrays.copyOf(nameAndAddition, 2);
			}

			return nameAndAddition;
		}

		private IArrayNode<IJsonNode> normalizeName(String name) {
			IArrayNode<IJsonNode> result = new ArrayNode<IJsonNode>(
					TextNode.EMPTY_STRING, TextNode.EMPTY_STRING,
					TextNode.EMPTY_STRING);
			if (name == null) {
				return result;
			}

			String[] nameParts = name.split(" ");

			String firstName = null;
			String middleName = null;
			String lastName = null;

			if (nameParts.length > 2) {
				middleName = "";
				for (int i = 1; i < nameParts.length - 1; i++) {
					middleName += " " + nameParts[i];
				}

				firstName = nameParts[0].trim();
				middleName = middleName.trim();
				lastName = nameParts[nameParts.length - 1].trim();
			} else if (nameParts.length == 2) {
				firstName = nameParts[0].trim();
				lastName = nameParts[1].trim();
			} else if (nameParts.length == 1) {
				lastName = nameParts[0].trim();
			}

			if (firstName != null)
				result.set(0, TextNode.valueOf(firstName));
			if (middleName != null)
				result.set(1, TextNode.valueOf(middleName));
			if (lastName != null)
				result.set(2, TextNode.valueOf(lastName));

			return result;
		}

	};

	private static class DictionarySearch {
		private String line, entry, found;

		private transient String dictionary;
		private transient File dictionaryFile;

		private int size, position;

		private int recordNr = 0;

		public String search(String searchStr, String dictionary, int where,
				boolean ignoreCase, boolean first) {
			if ((searchStr == null) || searchStr.equals("")) {
				return null;
			}
			if (this.dictionary == null || !this.dictionary.equals(dictionary)) {
				this.dictionaryFile = new File(dictionary);
				this.dictionary = dictionary;
			}
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(this.dictionaryFile));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				return null;
			}

			this.found = null;
			this.size = 0;

			searchStr = format(searchStr, ignoreCase);

			try {
				while ((this.line = br.readLine()) != null) {
					this.entry = format(this.line, ignoreCase);

					this.position = where > 0 ? searchStr
							.lastIndexOf(this.entry) : searchStr
							.indexOf(this.entry);

					if (this.position < 0
							|| (where > 0 && (this.position + this.entry
									.length()) < searchStr.length())
							|| (where < 0 && this.position > 0)) {
						continue;
					}

					if (this.line.length() > this.size) {
						this.found = this.line;
						this.size = this.line.length();

						if (first && this.size > 0) {
							break;
						}
					}
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}

			return this.found;
		}

		private String format(String input, boolean ignoreCase) {
			input = " " + input.trim() + " ";

			if (ignoreCase) {
				input = input.toLowerCase();
			}

			return input;
		}
	}

	@Name(noun = "dict_replace")
	public static class DICTIONARY_REPLACEMENT extends
			SopremoFunction3<TextNode, IJsonNode, TextNode> {

		private String DICT_ENDING = ".dict";

		private String DEFAULT_DICTIONARY_PATH = System.getProperty("user.dir")
				+ "/resources/dictionaries/";

		private String KEY_VALUE_SEPARATOR = "\\|";

		private String VALUE_SEPARATOR = ";";

		@Override
		protected IJsonNode call(TextNode inputNode, IJsonNode dictionaryPath,
				TextNode dictNameNode) {
			Map<String, String> mappings = this
					.loadDictionary(
							(dictionaryPath instanceof NullNode) ? this.DEFAULT_DICTIONARY_PATH
									: dictionaryPath.toString(), dictNameNode
									.toString());
			String input = inputNode.toString();
			String result = input;
			String sub;

			if (mappings.containsKey(input)) {
				result = mappings.get(input);
			} else if (mappings.containsValue(input)) {
				result = input;
			} else if ((sub = this.mappingSimilarTo(input, mappings)) != null) {
				result = sub;
			} else {
				// for unknown entities
				result = input;
			}
			return TextNode.valueOf(result);
		}

		// we also map similar strings
		private String mappingSimilarTo(String input,
				Map<String, String> mappings) {
			for (String value : mappings.values()) {
				if (input.startsWith(value) || input.endsWith(value)) {
					return value;
				}
			}
			return null;
		}

		private Map<String, String> loadDictionary(String dictionaryPath,
				String dictionaryName) {
			Map<String, String> mappings = new HashMap<String, String>();
			if (dictionaryName == null || "".equals(dictionaryName)) {
				return mappings;
			}
			try {
				BufferedReader br = new BufferedReader(new FileReader(
						dictionaryPath + dictionaryName + this.DICT_ENDING));
				String line, key;
				String[] keyValues, values;

				try {
					while ((line = br.readLine()) != null) {
						keyValues = line.split(this.KEY_VALUE_SEPARATOR);
						key = keyValues[0];
						values = keyValues[1].split(this.VALUE_SEPARATOR);
						for (String value : values) {
							// we have to put the key-value pairs in reversed
							// order to allow multiple mappings from the
							// same shortcut (key) to different spellings
							// (values)
							mappings.put(value, key);
						}
					}
					br.close();
				} catch (IOException e) {
					e.printStackTrace();

					// if an error occurs we just return an empty map to allow
					// further execution of the meteor script
					return new HashMap<String, String>();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();

				// if the specified dictionary doesn't exist we return the empty
				// map to allow further execution of the
				// meteor script
				return mappings;
			}
			return mappings;
		}
	};

	@Name(noun = "parsePhoneNumbers")
	public static class PARSE_PHONE_NUMBERS extends SopremoFunction1<TextNode> {

		@Override
		protected IJsonNode call(TextNode valueNode) {
			Pattern numberGroup3 = Pattern.compile("\\d{3}");
			IArrayNode<TextNode> result = new ArrayNode<TextNode>();
			String value = valueNode.toString();

			boolean repeat = true;
			String number = "";
			int groupNr = 0;

			while (repeat) {
				Matcher m3 = numberGroup3.matcher(value);
				if (m3.find()) {
					if ("0123456789".contains(String.valueOf(value.charAt(m3
							.end())))) {
						number = number + " "
								+ value.substring(m3.start(), m3.start() + 4);
					} else {
						number = number + " "
								+ value.substring(m3.start(), m3.start() + 3);
					}
					value = value.substring(m3.end());
				} else {
					repeat = false;
				}
				if (groupNr == 2) {
					groupNr = -1;
					number = number.trim();
					String[] numberSplit = number.split(" ");
					result.add(TextNode.valueOf(numberSplit[0] + "-"
							+ numberSplit[1] + "-" + numberSplit[2]));
					number = "";
				}
				groupNr++;
			}

			return result;
		}
	};

	@Name(noun = "generateId")
	public static class GENERATE_ID extends SopremoFunction0 {

		@Override
		protected IJsonNode call() {
			return TextNode.valueOf(UUID.randomUUID().toString());
		}
	};
}
