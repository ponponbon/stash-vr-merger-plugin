#!/usr/bin/env python3
import os
import re
import sys
import time
import json
import math
import pathlib
import requests
from typing import Dict, List, Any, Tuple, Optional

def get_plugin_input():
    """Read and parse plugin input from stdin"""
    try:
        input_data = json.loads(sys.stdin.read())
        return input_data
    except:
        # Fallback for direct execution
        return {
            "server_connection": {
                "Scheme": "http",
                "Port": 9999
            },
            "args": {}
        }

def normalize_graphql_url(url):
    """Normalize URL to ensure it ends with /graphql"""
    if not url:
        return url
    url = url.rstrip('/')
    if not url.endswith('/graphql'):
        url += '/graphql'
    return url

def get_stash_url(plugin_input, server_connection):
    """Build Stash URL with proper fallback priority"""
    # Priority 1: Plugin args (stash_url setting)
    args = plugin_input.get("args", {})
    if "stash_url" in args and args["stash_url"]:
        url = normalize_graphql_url(args["stash_url"])
        return url, "plugin arg"
    
    # Priority 2: Environment variable STASH_URL
    env_url = os.environ.get("STASH_URL")
    if env_url:
        url = normalize_graphql_url(env_url)
        return url, "environment variable"
    
    # Priority 3: Auto-build from server_connection
    if server_connection:
        scheme = server_connection.get("Scheme", "http")
        host = server_connection.get("Host", "localhost")
        port = server_connection.get("Port", 9999)
        url = f"{scheme}://{host}:{port}/graphql"
        return url, "server_connection"
    
    # Priority 4: Final fallback to localhost
    url = "http://localhost:9999/graphql"
    return url, "localhost fallback"

def get_plugin_setting(plugin_input, setting_name, default_value):
    """Get plugin setting value with fallback to environment variable and default"""
    # Try plugin args first
    args = plugin_input.get("args", {})
    if setting_name in args and args[setting_name] is not None:
        return args[setting_name]
    
    # Try environment variable
    env_name = setting_name.upper()
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return env_value
    
    return default_value

def test_graphql_connection(session, url):
    """Test GraphQL connection and return Stash version info"""
    test_query = """
    query {
      version {
        version
        build_time
      }
    }"""
    
    try:
        data = {"query": test_query}
        resp = session.post(url, data=json.dumps(data), timeout=10)
        resp.raise_for_status()
        
        result = resp.json()
        if "errors" in result:
            return False, f"GraphQL errors: {result['errors']}"
        
        version_info = result.get("data", {}).get("version", {})
        version = version_info.get("version", "unknown")
        build_time = version_info.get("build_time", "unknown")
        
        return True, f"Stash v{version} (built: {build_time})"
        
    except requests.exceptions.Timeout:
        return False, "Connection timeout (10s)"
    except requests.exceptions.ConnectionError:
        return False, "Connection refused - check URL and network"
    except requests.exceptions.HTTPError as e:
        return False, f"HTTP {e.response.status_code}: {e.response.reason}"
    except json.JSONDecodeError:
        return False, "Invalid JSON response - not a GraphQL endpoint"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def output_result(error=None, output=None):
    """Output plugin result in expected format"""
    result = {}
    if error:
        result["error"] = str(error)
    if output:
        result["output"] = output
    print(json.dumps(result))

# Initialize plugin
plugin_input = get_plugin_input()
server_connection = plugin_input.get("server_connection", {})
args = plugin_input.get("args", {})

STASH_URL, URL_SOURCE = get_stash_url(plugin_input, server_connection)
API_KEY = get_plugin_setting(plugin_input, "api_key", os.environ.get("STASH_API_KEY", ""))
VR_TAG_NAME = get_plugin_setting(plugin_input, "vr_tag_name", "VR")
MULTIPART_TAG_NAME = get_plugin_setting(plugin_input, "multipart_tag_name", "Multipart")
TEST_CONNECTION = get_plugin_setting(plugin_input, "test_connection", "false").lower() == "true"

# Check if this is preview mode
mode = args.get("mode", "merge")
DRY_RUN = (mode == "preview") or (get_plugin_setting(plugin_input, "dry_run", "false").lower() == "true")

SESSION = requests.Session()
if API_KEY:
    SESSION.headers.update({"ApiKey": API_KEY})

# Handle session cookie if provided (fallback when no API key)
if not API_KEY:
    session_cookie = server_connection.get("SessionCookie")
    if session_cookie:
        SESSION.cookies.set(
            session_cookie.get("Name", "session"),
            session_cookie.get("Value", ""),
            domain=session_cookie.get("Domain", "localhost"),
            path=session_cookie.get("Path", "/")
        )

SESSION.headers.update({"Content-Type": "application/json"})

# Regex to detect parts (pt1, part-02, cd2, disc iii, A/B)
PART_TOKEN = re.compile(
    r"(?ix)"
    r"(?:^|[ _.\-\(\)\[\]])"
    r"(?:pt|part|cd|disc)"
    r"[ _.\-]*"
    r"(?P<num>(?:\d{1,2}|[ivx]{1,6}))"
    r"(?:$|[ _.\-\(\)\[\]])"
)

# Also handle simple A/B split, e.g., Title A, Title B
AB_TOKEN = re.compile(r"(?i)(?:^|[ _.\-\(\)\[\]])(?P<ab>[AB])(?:$|[ _.\-\(\)\[\]])")

def roman_to_int(s: str) -> Optional[int]:
    s = s.upper()
    numerals = {'I':1, 'V':5, 'X':10}
    prev = 0
    total = 0
    for ch in reversed(s):
        val = numerals.get(ch, 0)
        if val < prev:
            total -= val
        else:
            total += val
            prev = val
    return total if total > 0 else None

def normalize_basename(name: str) -> Tuple[str, Optional[int]]:
    """
    Return (base_without_part, part_number or None).
    We strip well-known part tokens; leave extension handling to caller.
    """
    stem = pathlib.Path(name).stem

    # Prefer explicit part pattern
    m = PART_TOKEN.search(stem)
    if m:
        num = m.group("num")
        try:
            part = int(num)
        except ValueError:
            part = roman_to_int(num) or None
        base = PART_TOKEN.sub(" ", stem)
        return re.sub(r"\s+", " ", base).strip(), part

    # Handle trailing single-letter A/B style
    m2 = AB_TOKEN.search(stem)
    if m2:
        letter = m2.group("ab").upper()
        part = 1 if letter == "A" else 2 if letter == "B" else None
        base = AB_TOKEN.sub(" ", stem)
        return re.sub(r"\s+", " ", base).strip(), part

    return stem, None

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    data = {"query": query, "variables": variables or {}}
    resp = SESSION.post(STASH_URL, data=json.dumps(data))
    resp.raise_for_status()
    out = resp.json()
    if "errors" in out:
        raise RuntimeError(out["errors"])
    return out["data"]

def get_or_create_tag(name: str) -> str:
    # Try find by name
    q = """
    query FindTags($filter: FindFilterType!) {
      findTags(filter: $filter) {
        tags { id name }
      }
    }"""
    data = gql(q, {"filter": {"q": name, "per_page": 100}})
    for t in data["findTags"]["tags"]:
        if t["name"].lower() == name.lower():
            return t["id"]
    # Create
    m = """
    mutation CreateTag($input: TagCreateInput!) {
      tagCreate(input: $input) { id name }
    }"""
    created = gql(m, {"input": {"name": name}})["tagCreate"]
    return created["id"]

def fetch_scenes_page(page: int, per_page: int = 200) -> Tuple[int, List[Dict[str, Any]]]:
    q = """
    query($page: Int!, $per_page: Int!) {
      findScenes(filter: {per_page: $per_page, page: $page}) {
        count
        scenes {
          id
          title
          files { id path basename }
          tags { id name }
        }
      }
    }"""
    data = gql(q, {"page": page, "per_page": per_page})["findScenes"]
    return data["count"], data["scenes"]

def scene_update_tags(scene_id: str, tag_ids: List[str]):
    m = """
    mutation UpdateSceneTags($input: SceneUpdateInput!) {
      sceneUpdate(input: $input) { id }
    }"""
    if DRY_RUN:
        print(f"[DRY] sceneUpdate tags for {scene_id}: {tag_ids}")
        return
    gql(m, {"input": {"id": scene_id, "tag_ids": tag_ids}})

def scene_update_title(scene_id: str, title: str):
    m = """
    mutation UpdateSceneTitle($input: SceneUpdateInput!) {
      sceneUpdate(input: $input) { id }
    }"""
    if DRY_RUN:
        print(f"[DRY] sceneUpdate title for {scene_id}: {title!r}")
        return
    gql(m, {"input": {"id": scene_id, "title": title}})

def scene_merge(target_id: str, source_ids: List[str]):
    if not source_ids:
        return
    m = """
    mutation MergeScenes($input: SceneMergeInput!) {
      sceneMerge(input: $input) { id }
    }"""
    if DRY_RUN:
        print(f"[DRY] sceneMerge target={target_id} sources={source_ids}")
        return
    gql(m, {"input": {"destination": target_id, "source": source_ids}})

def main():
    try:
        print("== Merge Multipart VR Scenes ==")
        print(f"Using GraphQL endpoint: {STASH_URL} (source: {URL_SOURCE})")
        print(f"DRY_RUN={DRY_RUN}")
        
        # Test connection if requested
        if TEST_CONNECTION:
            print("Testing GraphQL connection...")
            success, message = test_graphql_connection(SESSION, STASH_URL)
            if success:
                print(f"✓ Connection successful: {message}")
            else:
                error_msg = f"✗ Connection failed: {message}"
                print(error_msg)
                output_result(error=error_msg)
                return
        
        vr_tag_id = get_or_create_tag(VR_TAG_NAME) if VR_TAG_NAME else None
        mp_tag_id = get_or_create_tag(MULTIPART_TAG_NAME)

        # Gather candidate scenes by scanning all scenes (paginate)
        page = 1
        per_page = 200
        total, scenes = fetch_scenes_page(page, per_page)
        pages = max(1, math.ceil(total / per_page))
        all_scenes = scenes[:]
        while page < pages:
            page += 1
            _, scenes = fetch_scenes_page(page, per_page)
            all_scenes.extend(scenes)

        # Build groups keyed by (dirpath, normalized_base) -> list[(scene, part, filename)]
        groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for sc in all_scenes:
            # If you're strict about VR only, uncomment next two lines
            # if vr_tag_id and not any(t["id"] == vr_tag_id for t in sc["tags"]):
            #     continue

            if not sc["files"]:
                continue
            # Use first file's directory as grouping anchor
            f = sc["files"][0]
            dirpath = str(pathlib.Path(f["path"]).parent)
            # Detect part from basename
            base, part = normalize_basename(f["basename"])
            if part is None:
                # Not a part; skip unless title indicates multi-part
                continue

            key = (dirpath, base.lower())
            entry = {"scene": sc, "part": part, "basename": f["basename"]}
            groups.setdefault(key, []).append(entry)

        # Plan merges for any group with >= 2 parts
        merged_count = 0
        merge_summary = []
        
        for key, items in groups.items():
            if len(items) < 2:
                continue
            # Sort by part
            items.sort(key=lambda x: x["part"])

            scene_ids = [it["scene"]["id"] for it in items]
            titles = [it["scene"]["title"] for it in items]
            dirpath, base = key
            
            merge_info = {
                "group": f"{dirpath} :: {base}",
                "parts": [it['part'] for it in items],
                "scene_ids": scene_ids,
                "titles": titles
            }
            merge_summary.append(merge_info)
            
            print(f"\nGroup: {dirpath} :: {base}  -> parts {[it['part'] for it in items]}")
            print(f"Scenes: {scene_ids}")
            
            # Choose target: the lowest part number scene
            target = items[0]["scene"]
            sources = [it["scene"]["id"] for it in items[1:]]
            
            # Merge
            scene_merge(target["id"], sources)
            merged_count += 1

            # Re-tag and retitle the target
            tag_ids = {t["id"] for t in target["tags"]}
            tag_ids.add(mp_tag_id)
            if vr_tag_id:
                tag_ids.add(vr_tag_id)
            scene_update_tags(target["id"], list(tag_ids))

            # Strip "part" from title if present
            new_title = re.sub(r"(?i)\b(?:pt|part|cd|disc)[ _.\-]*\d+\b", "", target["title"]).strip()
            new_title = re.sub(r"\s{2,}", " ", new_title)
            if new_title and new_title != target["title"]:
                scene_update_title(target["id"], new_title)

        result_message = f"Done. Groups merged: {merged_count}"
        print(f"\n{result_message}")
        
        if DRY_RUN:
            dry_run_msg = "DRY_RUN was ON. Use 'merge_vr_videos' task to apply changes."
            print(dry_run_msg)
            result_message += f"\n{dry_run_msg}"

        # Output plugin result
        output_result(output={
            "message": result_message,
            "merged_count": merged_count,
            "merge_summary": merge_summary,
            "dry_run": DRY_RUN
        })

    except requests.HTTPError as e:
        error_msg = f"HTTP error talking to Stash GraphQL: {e}"
        print(error_msg, file=sys.stderr)
        output_result(error=error_msg)
        sys.exit(2)
    except Exception as e:
        error_msg = f"Error: {e}"
        print(error_msg, file=sys.stderr)
        output_result(error=error_msg)
        sys.exit(1)

if __name__ == "__main__":
    main()
