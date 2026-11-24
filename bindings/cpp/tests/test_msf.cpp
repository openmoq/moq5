#include <moq/msf.hpp>
#include "test_support.hpp"

#include <cstring>
#include <span>
#include <string>
#include <string_view>

int main()
{
    int failures = 0;

    // Parse minimal JSON
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true}]})"));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->version() == 1);
        MOQ_CHECK(r->track_count() == 1);
        auto t = r->track(0);
        MOQ_CHECK(t.name == "v");
        MOQ_CHECK(t.packaging == "loc");
        MOQ_CHECK(t.is_live == true);
        // Independent tracks always carry the required packaging/isLive keys.
        MOQ_CHECK(t.has_packaging && t.has_is_live);
        // An absent array yields an empty, null-safe iterable span.
        MOQ_CHECK(t.depends.empty() && t.depends.size() == 0);
        int dn = 0;
        for (auto d : t.depends) { (void)d; dn++; }
        MOQ_CHECK(dn == 0);
    }

    // find_role
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"tracks":[)"
            R"({"name":"v","packaging":"loc","isLive":true,"role":"video"},)"
            R"({"name":"a","packaging":"loc","isLive":true,"role":"audio"}]})"));
        MOQ_CHECK(r.ok());
        auto v = r->find_role("video");
        MOQ_CHECK(v.has_value());
        MOQ_CHECK(v->name == "v");
        auto a = r->find_role("audio");
        MOQ_CHECK(a.has_value());
        MOQ_CHECK(a->name == "a");
        MOQ_CHECK(!r->find_role("subtitle").has_value());
    }

    // generated_at optional
    {
        auto r1 = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"tracks":[]})"));
        MOQ_CHECK(r1.ok());
        MOQ_CHECK(!r1->generated_at().has_value());

        auto r2 = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"generatedAt":1746104606044,"tracks":[]})"));
        MOQ_CHECK(r2.ok());
        MOQ_CHECK(r2->generated_at().has_value());
        MOQ_CHECK(*r2->generated_at() == 1746104606044ULL);
    }

    // Parse invalid JSON returns error
    {
        auto r = moq::msf::parse(moq::bytes_view("{not valid json"));
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::protocol);
    }

    // Parsed string views valid until destruction
    {
        std::string_view name_sv;
        {
            auto r = moq::msf::parse(moq::bytes_view(
                R"({"version":1,"tracks":[{"name":"longtrack","packaging":"loc","isLive":false}]})"));
            MOQ_CHECK(r.ok());
            name_sv = r->track(0).name;
            MOQ_CHECK(name_sv == "longtrack");
        }
        // name_sv is now dangling — do not access
    }

    // Encode empty catalog, parse back
    {
        moq::msf::catalog cat;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        MOQ_CHECK(!enc->empty());

        auto dec = moq::msf::parse(
            moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->track_count() == 0);
    }

    // Encode one video track, parse back
    {
        moq::msf::track t;
        t.name = "video";
        t.packaging = "loc";
        t.is_live = true;
        t.has_role = true; t.role = "video";
        t.has_codec = true; t.codec = "avc1.42e01e";
        t.has_width = true; t.width = 1920;
        t.has_height = true; t.height = 1080;

        moq::msf::catalog cat;
        cat.tracks = &t;
        cat.track_count = 1;

        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());

        auto dec = moq::msf::parse(
            moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->track_count() == 1);
        auto tv = dec->track(0);
        MOQ_CHECK(tv.name == "video");
        MOQ_CHECK(tv.role.has_value() && *tv.role == "video");
        MOQ_CHECK(tv.codec.has_value() && *tv.codec == "avc1.42e01e");
        MOQ_CHECK(tv.width.has_value() && *tv.width == 1920);
        MOQ_CHECK(tv.height.has_value() && *tv.height == 1080);
    }

    // String needing JSON escaping round-trips
    {
        moq::msf::track t;
        t.name = "video";
        t.packaging = "loc";
        t.is_live = true;
        t.has_label = true;
        t.label = moq::bytes_view("He\"l\\lo\n");

        moq::msf::catalog cat;
        cat.tracks = &t;
        cat.track_count = 1;

        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());

        auto dec = moq::msf::parse(
            moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        auto tv = dec->track(0);
        MOQ_CHECK(tv.label.has_value());
        MOQ_CHECK(*tv.label == "He\"l\\lo\n");
    }

    // Encode returns buffer via adopt (non-null raw)
    {
        moq::msf::catalog cat;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        MOQ_CHECK(enc->raw() != nullptr);
        MOQ_CHECK(enc->size() > 0);
    }

    // Move parsed_catalog
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true}]})"));
        MOQ_CHECK(r.ok());

        moq::msf::parsed_catalog moved = std::move(*r);
        MOQ_CHECK(moved.version() == 1);
        MOQ_CHECK(moved.track_count() == 1);
        MOQ_CHECK(moved.track(0).name == "v");
    }

    // Move buffer from encode
    {
        moq::msf::catalog cat;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());

        moq::buffer b = std::move(*enc);
        MOQ_CHECK(!b.empty());
        MOQ_CHECK(enc->empty());
    }

    // Base64 decode
    {
        auto r = moq::msf::decode_init_data(moq::bytes_view("Zm9vYmFy"));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->size() == 6);
        MOQ_CHECK(std::memcmp(r->data(), "foobar", 6) == 0);
    }

    // Base64 encode
    {
        uint8_t raw[] = {0x01, 0x64, 0x00};
        auto r = moq::msf::encode_init_data(moq::bytes_view(raw, sizeof(raw)));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->size() == 4);
        MOQ_CHECK(std::memcmp(r->data(), "AWQA", 4) == 0);
    }

    // Base64 round-trip
    {
        uint8_t raw[] = {0xFF, 0x00, 0xAB, 0xCD};
        auto enc = moq::msf::encode_init_data(moq::bytes_view(raw, sizeof(raw)));
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::decode_init_data(
            moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->size() == sizeof(raw));
        MOQ_CHECK(std::memcmp(dec->data(), raw, sizeof(raw)) == 0);
    }

    // Base64 decode malformed
    {
        auto r = moq::msf::decode_init_data(moq::bytes_view("Z@=="));
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::protocol);
    }

    // MSF-01/CMSF-01 scalar field parity: author + round-trip the recently
    // added track scalars and catalog isComplete.
    {
        moq::msf::track t;
        t.name = "v"; t.packaging = "cmaf"; t.is_live = false;
        t.has_codec = true; t.codec = "avc1.640028";
        t.has_bitrate = true; t.bitrate = 1500000;
        t.has_init_ref = true; t.init_ref = "init-v";
        t.has_mime_type = true; t.mime_type = "video/mp4";
        t.has_track_duration = true; t.track_duration_ms = 8072;
        t.has_max_grp_sap = true; t.max_grp_sap = 1;
        t.has_max_obj_sap = true; t.max_obj_sap = 2;

        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        cat.is_complete = true;

        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->is_complete());
        auto tv = dec->track(0);
        MOQ_CHECK(tv.init_ref.has_value() && *tv.init_ref == "init-v");
        MOQ_CHECK(tv.mime_type.has_value() && *tv.mime_type == "video/mp4");
        MOQ_CHECK(tv.track_duration_ms.has_value() && *tv.track_duration_ms == 8072);
        MOQ_CHECK(tv.max_grp_sap.has_value() && *tv.max_grp_sap == 1);
        MOQ_CHECK(tv.max_obj_sap.has_value() && *tv.max_obj_sap == 2);
    }

    // Decode a C-compatible eventtimeline track exposing eventType/mimeType.
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":"1","tracks":[{"name":"v.sap",)"
            R"("packaging":"eventtimeline","isLive":false,)"
            R"("eventType":"org.ietf.moq.cmsf.sap","mimeType":"application/json"}]})"));
        MOQ_CHECK(r.ok());
        auto tv = r->track(0);
        MOQ_CHECK(tv.event_type.has_value() && *tv.event_type == "org.ietf.moq.cmsf.sap");
        MOQ_CHECK(tv.mime_type.has_value() && *tv.mime_type == "application/json");
        MOQ_CHECK(!r->is_complete());   // absent => false
    }

    // MSF-01 5.1.7 root initDataList + initRef resolution.
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"tracks":[)"
            R"({"name":"hd","packaging":"cmaf","isLive":true,"initRef":"init-hd"}],)"
            R"("initDataList":[{"id":"init-hd","type":"inline","data":"AAAB"}]})"));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->init_data_count() == 1);
        auto e = r->init_data(0);
        MOQ_CHECK(e.id == "init-hd" && e.type == "inline" && e.data == "AAAB");
        auto tv = r->track(0);
        MOQ_CHECK(tv.init_ref.has_value());
        auto resolved = r->find_init_data(*tv.init_ref);
        MOQ_CHECK(resolved.has_value() && resolved->data == "AAAB");
        MOQ_CHECK(!r->find_init_data("nope").has_value());
    }

    // CMSF 4.1.1 contentProtections + nested drmSystem + track refIDs (parse is
    // lenient: non-UUID kids are carried as-is on the read path).
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":"1","contentProtections":[{)"
            R"("refID":"cp1","defaultKID":["kid-a","kid-b"],"scheme":"cbcs",)"
            R"("drmSystem":{"systemID":"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed",)"
            R"("laURL":{"url":"https://la.example/x","type":"EME-1.0"},)"
            R"("certURL":{"url":"https://cert.example/y"},)"
            R"("pssh":"AAAB","robustness":"HW_SECURE_ALL"}}],)"
            R"("tracks":[{"name":"v","packaging":"cmaf","isLive":true,)"
            R"("contentProtectionRefIDs":["cp1"]}]})"));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->content_protection_count() == 1);
        auto cp = r->content_protection(0);
        MOQ_CHECK(cp.ref_id == "cp1");
        MOQ_CHECK(cp.scheme == "cbcs");
        MOQ_CHECK(cp.default_kids.size() == 2);
        MOQ_CHECK(cp.default_kids[0] == "kid-a" && cp.default_kids[1] == "kid-b");
        MOQ_CHECK(cp.drm_system.system_id == "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
        MOQ_CHECK(cp.drm_system.la_url.has_value());
        MOQ_CHECK(cp.drm_system.la_url->url == "https://la.example/x");
        MOQ_CHECK(cp.drm_system.la_url->type.has_value() &&
                  *cp.drm_system.la_url->type == "EME-1.0");
        MOQ_CHECK(cp.drm_system.cert_url.has_value() &&
                  !cp.drm_system.cert_url->type.has_value());
        MOQ_CHECK(!cp.drm_system.auth_url.has_value());
        MOQ_CHECK(cp.drm_system.pssh.has_value() && *cp.drm_system.pssh == "AAAB");
        MOQ_CHECK(cp.drm_system.robustness.has_value() &&
                  *cp.drm_system.robustness == "HW_SECURE_ALL");

        auto tv = r->track(0);
        MOQ_CHECK(tv.content_protection_ref_ids.size() == 1);
        MOQ_CHECK(tv.content_protection_ref_ids[0] == "cp1");
        auto found = r->find_content_protection(tv.content_protection_ref_ids[0]);
        MOQ_CHECK(found.has_value() && found->ref_id == "cp1");
        MOQ_CHECK(!r->find_content_protection("nope").has_value());
    }

    // MSF-01 5.2.14 depends[].
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"tracks":[{"name":"cc","packaging":"loc",)"
            R"("isLive":true,"depends":["v","a"]}]})"));
        MOQ_CHECK(r.ok());
        auto tv = r->track(0);
        MOQ_CHECK(tv.depends.size() == 2);
        MOQ_CHECK(tv.depends[0] == "v" && tv.depends[1] == "a");
        // range-for over the span
        int n = 0;
        for (auto d : tv.depends) { (void)d; n++; }
        MOQ_CHECK(n == 2);
    }

    // MSF-01 5.2.15 / §7.4 media-timeline template: round-trip via authoring.
    {
        moq::msf::track t;
        t.name = "v"; t.packaging = "cmaf"; t.is_live = false;
        t.has_timeline_template = true;
        t.timeline_template.delta_media_ms     = 2002;
        t.timeline_template.delta_group        = 1;
        t.timeline_template.start_wallclock_ms = 1759924158381ULL;
        t.timeline_template.delta_wallclock_ms = 2002;

        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        auto tv = dec->track(0);
        MOQ_CHECK(tv.timeline_template.has_value());
        MOQ_CHECK(tv.timeline_template->delta_media_ms == 2002);
        MOQ_CHECK(tv.timeline_template->delta_group == 1);
        MOQ_CHECK(tv.timeline_template->start_wallclock_ms == 1759924158381ULL);
        MOQ_CHECK(tv.timeline_template->delta_wallclock_ms == 2002);
    }

    // MSF-01 5.1.6 deltaUpdate read parity: add + clone (parentName/overrides).
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"generatedAt":1,"deltaUpdate":[)"
            R"({"op":"add","tracks":[{"name":"slides","isLive":false}]},)"
            R"({"op":"clone","tracks":[{"name":"video-720",)"
            R"("parentName":"video-1080","parentNamespace":"example.com/custom",)"
            R"("width":1280,"bitrate":600000}]}]})"));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->is_delta());
        MOQ_CHECK(r->track_count() == 0);
        MOQ_CHECK(r->delta_op_count() == 2);

        auto add = r->delta_op(0);
        MOQ_CHECK(add.kind() == moq::msf::delta_op_kind::add);
        MOQ_CHECK(add.track_count() == 1);
        MOQ_CHECK(add.track(0).name == "slides");
        // add track carries an explicit isLive but omits packaging.
        MOQ_CHECK(add.track(0).has_is_live && !add.track(0).has_packaging);

        auto clone = r->delta_op(1);
        MOQ_CHECK(clone.kind() == moq::msf::delta_op_kind::clone);
        auto ct = clone.track(0);
        MOQ_CHECK(ct.name == "video-720");
        // clone omits both packaging and isLive -> inherited, not explicit false.
        MOQ_CHECK(!ct.has_packaging && !ct.has_is_live);
        MOQ_CHECK(ct.parent_name.has_value() && *ct.parent_name == "video-1080");
        MOQ_CHECK(ct.parent_namespace.has_value() &&
                  *ct.parent_namespace == "example.com/custom");
        MOQ_CHECK(ct.width.has_value() && *ct.width == 1280);
        MOQ_CHECK(ct.bitrate.has_value() && *ct.bitrate == 600000);
    }

    // deltaUpdate remove op.
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"deltaUpdate":[{"op":"remove","tracks":[)"
            R"({"name":"video"},{"name":"slides"}]}]})"));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->delta_op_count() == 1);
        auto rm = r->delta_op(0);
        MOQ_CHECK(rm.kind() == moq::msf::delta_op_kind::remove);
        MOQ_CHECK(rm.track_count() == 2);
        MOQ_CHECK(rm.track(0).name == "video" && rm.track(1).name == "slides");
        // An independent catalog reports is_delta() == false.
        auto indep = moq::msf::parse(moq::bytes_view(
            R"({"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true}]})"));
        MOQ_CHECK(indep.ok() && !indep->is_delta());
    }

    // ---- Authoring parity (nested/array fields) -------------------------

    // MSF-01 5.2.14 depends[] authoring round-trip.
    {
        moq::bytes_view deps[] = { moq::bytes_view("v"), moq::bytes_view("a") };
        moq::msf::track t;
        t.name = "cc"; t.packaging = "loc"; t.is_live = true;
        t.depends = deps; t.depends_count = 2;

        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        auto tv = dec->track(0);
        MOQ_CHECK(tv.depends.size() == 2);
        MOQ_CHECK(tv.depends[0] == "v" && tv.depends[1] == "a");
    }

    // MSF-01 5.1.7 initDataList authoring + initRef resolution round-trip.
    {
        moq::msf::init_data_entry idl{ moq::bytes_view("init-hd"),
                                       moq::bytes_view("inline"),
                                       moq::bytes_view("AAAB") };
        moq::msf::track t;
        t.name = "hd"; t.packaging = "cmaf"; t.is_live = true;
        t.has_init_ref = true; t.init_ref = "init-hd";

        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        cat.init_data_list = &idl; cat.init_data_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->init_data_count() == 1);
        auto e = dec->init_data(0);
        MOQ_CHECK(e.id == "init-hd" && e.type == "inline" && e.data == "AAAB");
        auto tv = dec->track(0);
        MOQ_CHECK(tv.init_ref.has_value());
        auto res = dec->find_init_data(*tv.init_ref);
        MOQ_CHECK(res.has_value() && res->data == "AAAB");
    }

    // CMSF 4.1.1 contentProtections + track refIDs authoring round-trip. The
    // public encoder enforces §4 syntax, so this uses valid UUIDs.
    {
        moq::bytes_view kids[] = {
            moq::bytes_view("01234567-89ab-cdef-0123-456789abcdef") };
        moq::msf::content_protection cp;
        cp.ref_id = "cp1";
        cp.default_kids = kids; cp.default_kid_count = 1;
        cp.scheme = "cenc";
        cp.drm.system_id = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed";
        cp.drm.has_la_url = true; cp.drm.la_url.url = "https://la.example/x";
        cp.drm.la_url.has_type = true; cp.drm.la_url.type = "EME-1.0";
        cp.drm.has_cert_url = true; cp.drm.cert_url.url = "https://cert.example/y";
        cp.drm.has_pssh = true; cp.drm.pssh = "AAAB";
        cp.drm.has_robustness = true; cp.drm.robustness = "HW_SECURE_ALL";

        moq::bytes_view refs[] = { moq::bytes_view("cp1") };
        moq::msf::track t;
        t.name = "v"; t.packaging = "cmaf"; t.is_live = true;
        t.content_protection_ref_ids = refs;
        t.content_protection_ref_id_count = 1;

        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        cat.content_protections = &cp; cat.content_protection_count = 1;

        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->content_protection_count() == 1);
        auto rcp = dec->content_protection(0);
        MOQ_CHECK(rcp.ref_id == "cp1" && rcp.scheme == "cenc");
        MOQ_CHECK(rcp.default_kids.size() == 1);
        MOQ_CHECK(rcp.drm_system.la_url.has_value() &&
                  rcp.drm_system.la_url->url == "https://la.example/x");
        MOQ_CHECK(rcp.drm_system.la_url->type.has_value() &&
                  *rcp.drm_system.la_url->type == "EME-1.0");
        MOQ_CHECK(rcp.drm_system.cert_url.has_value());
        MOQ_CHECK(rcp.drm_system.pssh.has_value() && *rcp.drm_system.pssh == "AAAB");
        MOQ_CHECK(rcp.drm_system.robustness.has_value());
        auto tv = dec->track(0);
        MOQ_CHECK(tv.content_protection_ref_ids.size() == 1 &&
                  tv.content_protection_ref_ids[0] == "cp1");
        MOQ_CHECK(dec->find_content_protection("cp1").has_value());
    }

    // CMSF §4: the public encoder rejects a non-enum scheme on authoring.
    {
        moq::bytes_view kids[] = {
            moq::bytes_view("01234567-89ab-cdef-0123-456789abcdef") };
        moq::msf::content_protection cp;
        cp.ref_id = "cp1";
        cp.default_kids = kids; cp.default_kid_count = 1;
        cp.scheme = "aes-128";   // not cenc/cbcs
        cp.drm.system_id = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed";

        moq::msf::track t;
        t.name = "v"; t.packaging = "cmaf"; t.is_live = true;
        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        cat.content_protections = &cp; cat.content_protection_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(!enc.ok());
        MOQ_CHECK(enc.error().code() == moq::errc::invalid);
    }

    // MSF-01 5.1.6 deltaUpdate authoring: add (partial) + clone (overrides).
    {
        moq::msf::track add_t;
        add_t.name = "slides";
        add_t.has_is_live = true; add_t.is_live = true;   // packaging omitted

        moq::msf::track clone_t;
        clone_t.name = "video-720";
        clone_t.has_parent_name = true; clone_t.parent_name = "video-1080";
        clone_t.has_parent_namespace = true;
        clone_t.parent_namespace = "example.com/custom";
        clone_t.has_width = true; clone_t.width = 1280;
        clone_t.has_bitrate = true; clone_t.bitrate = 600000;

        moq::msf::delta_op ops[2];
        ops[0].kind = moq::msf::delta_op_kind::add;
        ops[0].tracks = &add_t; ops[0].track_count = 1;
        ops[1].kind = moq::msf::delta_op_kind::clone;
        ops[1].tracks = &clone_t; ops[1].track_count = 1;

        moq::msf::catalog cat;
        cat.delta_update = ops; cat.delta_update_count = 2;
        cat.has_generated_at = true; cat.generated_at = 1;

        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->is_delta() && dec->delta_op_count() == 2);

        auto a = dec->delta_op(0);
        MOQ_CHECK(a.kind() == moq::msf::delta_op_kind::add);
        MOQ_CHECK(a.track(0).name == "slides");
        MOQ_CHECK(a.track(0).has_is_live && !a.track(0).has_packaging);

        auto c = dec->delta_op(1);
        MOQ_CHECK(c.kind() == moq::msf::delta_op_kind::clone);
        auto cv = c.track(0);
        MOQ_CHECK(cv.name == "video-720");
        MOQ_CHECK(cv.parent_name.has_value() && *cv.parent_name == "video-1080");
        MOQ_CHECK(cv.parent_namespace.has_value() &&
                  *cv.parent_namespace == "example.com/custom");
        MOQ_CHECK(cv.width.has_value() && *cv.width == 1280);
        MOQ_CHECK(cv.bitrate.has_value() && *cv.bitrate == 600000);
    }

    // deltaUpdate remove op authoring (name-only tracks).
    {
        moq::msf::track rtracks[2];
        rtracks[0].name = "video";
        rtracks[1].name = "slides";
        moq::msf::delta_op op;
        op.kind = moq::msf::delta_op_kind::remove;
        op.tracks = rtracks; op.track_count = 2;

        moq::msf::catalog cat;
        cat.delta_update = &op; cat.delta_update_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->delta_op_count() == 1);
        auto rm = dec->delta_op(0);
        MOQ_CHECK(rm.kind() == moq::msf::delta_op_kind::remove);
        MOQ_CHECK(rm.track_count() == 2);
        MOQ_CHECK(rm.track(0).name == "video" && rm.track(1).name == "slides");
    }

    // Bad-input hardening: a non-zero count with a null array pointer must yield
    // errc::invalid, not a crash (root track depends, CP defaultKID).
    {
        moq::msf::track t;
        t.name = "v"; t.packaging = "loc"; t.is_live = true;
        t.depends = nullptr; t.depends_count = 2;   // incoherent
        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(!enc.ok());
        MOQ_CHECK(enc.error().code() == moq::errc::invalid);
    }
    {
        moq::msf::track t;
        t.name = "v"; t.packaging = "loc"; t.is_live = true;
        t.content_protection_ref_ids = nullptr;
        t.content_protection_ref_id_count = 1;       // incoherent
        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(!enc.ok());
        MOQ_CHECK(enc.error().code() == moq::errc::invalid);
    }
    {
        moq::msf::content_protection cp;
        cp.ref_id = "cp1";
        cp.default_kids = nullptr; cp.default_kid_count = 1;   // incoherent
        cp.scheme = "cenc";
        cp.drm.system_id = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed";
        moq::msf::track t;
        t.name = "v"; t.packaging = "cmaf"; t.is_live = true;
        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        cat.content_protections = &cp; cat.content_protection_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(!enc.ok());
        MOQ_CHECK(enc.error().code() == moq::errc::invalid);
    }
    {
        // delta-op track with an incoherent depends array.
        moq::msf::track add_t;
        add_t.name = "slides";
        add_t.has_is_live = true; add_t.is_live = true;
        add_t.depends = nullptr; add_t.depends_count = 3;   // incoherent
        moq::msf::delta_op op;
        op.kind = moq::msf::delta_op_kind::add;
        op.tracks = &add_t; op.track_count = 1;
        moq::msf::catalog cat;
        cat.delta_update = &op; cat.delta_update_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(!enc.ok());
        MOQ_CHECK(enc.error().code() == moq::errc::invalid);
    }

    // MSF-02: the revision, publish tracks, an update op and the supplement.
    {
        auto r = moq::msf::parse(moq::bytes_view(
            R"({"version":"draft-02","tracks":[{"name":"v","packaging":"loc",)"
            R"("isLive":true,"keyId":"k","keyframeSpacing":15}],)"
            R"("publishTracks":[{"name":"log","packaging":"moqlog","isLive":true}]})"));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->revision() == MOQ_MSF_REVISION_02);
        MOQ_CHECK(r->publish_track_count() == 1);
        MOQ_CHECK(r->publish_track(0).name == "log");
        auto tv = r->track(0);
        MOQ_CHECK(tv.supplement != nullptr);
        if (tv.supplement) {
            MOQ_CHECK(tv.supplement->has_keyframe_spacing &&
                      tv.supplement->keyframe_spacing == 15);
            MOQ_CHECK(moq::msf::detail::sv(tv.supplement->key_id) == "k");
        }
        MOQ_CHECK(r->publish_track(0).supplement == nullptr);

        auto d = moq::msf::parse(moq::bytes_view(
            R"({"generatedAt":1,"deltaUpdate":[{"op":"update","tracks":[)"
            R"({"parentName":"v","bitrate":5}]}]})"));
        MOQ_CHECK(d.ok());
        MOQ_CHECK(d->revision() == MOQ_MSF_REVISION_02);
        MOQ_CHECK(d->delta_op(0).kind() == moq::msf::delta_op_kind::update);
        MOQ_CHECK(d->delta_op(0).track(0).parent_name.value_or("") == "v");

        auto old = moq::msf::parse(moq::bytes_view(
            R"({"version":"1","tracks":[]})"));
        MOQ_CHECK(old.ok() && old->revision() == MOQ_MSF_REVISION_01);
    }

    // MSF-02 authoring: the supplement fields, Number "depends" entries, the
    // revision and the publish tracks survive encode and parse.
    {
        moq::msf::track t[2];
        t[0].name = "video"; t[0].packaging = "loc"; t[0].is_live = true;
        t[0].has_render_group = true; t[0].render_group = 1;
        t[0].has_alt_group = true; t[0].alt_group = 4;
        t[0].has_packaging_metadata = true;
        t[0].packaging_metadata = "streamMapping=object";
        t[0].keyframe_spacing = 30;
        t[0].accessible_window_ms = 60000;
        t[0].has_encryption_scheme = true; t[0].encryption_scheme = "moq-secure-objects";
        t[0].has_cipher_suite = true; t[0].cipher_suite = "aes-128-gcm-sha256";
        t[0].has_key_id = true; t[0].key_id = "kid-1";
        t[0].has_track_base_key = true; t[0].track_base_key = "MTIzNDU2Nzg5MDEyMzQ1Ng==";
        const moq::bytes_view deps[] = {"video"};
        t[1].name = "captions"; t[1].packaging = "loc"; t[1].is_live = true;
        t[1].depends = deps; t[1].depends_count = 1;
        t[1].depends_render_groups = {1};
        moq::msf::track pub;
        pub.name = "qoe"; pub.packaging = "moqlog"; pub.is_live = true;

        moq::msf::catalog cat;
        cat.revision = MOQ_MSF_REVISION_02;
        cat.tracks = t; cat.track_count = 2;
        cat.publish_tracks = &pub; cat.publish_track_count = 1;
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        if (enc.ok()) {
            std::string_view json(reinterpret_cast<const char *>(enc->data()), enc->size());
            MOQ_CHECK(json.find(R"("version":"draft-02")") != std::string_view::npos);
            MOQ_CHECK(json.find(R"("depends":["video",1])") != std::string_view::npos);
            auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
            MOQ_CHECK(dec.ok());
            if (dec.ok()) {
                MOQ_CHECK(dec->revision() == MOQ_MSF_REVISION_02);
                MOQ_CHECK(dec->publish_track_count() == 1);
                MOQ_CHECK(dec->publish_track(0).name == "qoe");
                MOQ_CHECK(dec->publish_track(0).supplement == nullptr);
                const moq_msf_track_supplement_t *s = dec->track(0).supplement;
                MOQ_CHECK(s != nullptr);
                if (s) {
                    namespace d = moq::msf::detail;
                    MOQ_CHECK(s->has_packaging_metadata &&
                              d::sv(s->packaging_metadata) == "streamMapping=object");
                    MOQ_CHECK(s->has_keyframe_spacing && s->keyframe_spacing == 30);
                    MOQ_CHECK(s->has_accessible_window_ms && s->accessible_window_ms == 60000);
                    MOQ_CHECK(s->has_encryption_scheme &&
                              d::sv(s->encryption_scheme) == "moq-secure-objects");
                    MOQ_CHECK(s->has_cipher_suite && d::sv(s->cipher_suite) == "aes-128-gcm-sha256");
                    MOQ_CHECK(s->has_key_id && d::sv(s->key_id) == "kid-1");
                    MOQ_CHECK(s->has_track_base_key &&
                              d::sv(s->track_base_key) == "MTIzNDU2Nzg5MDEyMzQ1Ng==");
                    MOQ_CHECK(s->depends_render_group_count == 0);
                }
                auto cv = dec->track(1);
                MOQ_CHECK(cv.depends.size() == 1 && cv.depends[0] == "video");
                MOQ_CHECK(cv.supplement != nullptr);
                if (cv.supplement) {
                    MOQ_CHECK(cv.supplement->depends_render_group_count == 1);
                    MOQ_CHECK(cv.supplement->depends_render_groups &&
                              cv.supplement->depends_render_groups[0] == 1);
                    MOQ_CHECK(!cv.supplement->has_keyframe_spacing &&
                              !cv.supplement->has_encryption_scheme);
                }
            }
        }
    }

    // MSF-01 encode: the four encryption fields are MSF-01 fields too and are
    // carried; the MSF-02-only fields are refused, and so are publish tracks
    // on a delta.
    {
        moq::msf::track t;
        t.name = "v"; t.packaging = "loc"; t.is_live = true;
        t.has_encryption_scheme = true; t.encryption_scheme = "moq-secure-objects";
        t.has_key_id = true; t.key_id = "k";
        moq::msf::catalog cat;
        cat.tracks = &t; cat.track_count = 1;
        MOQ_CHECK(cat.revision == MOQ_MSF_REVISION_01);
        auto enc = moq::msf::encode(cat);
        MOQ_CHECK(enc.ok());
        if (enc.ok()) {
            auto dec = moq::msf::parse(moq::bytes_view(enc->data(), enc->size()));
            MOQ_CHECK(dec.ok() && dec->revision() == MOQ_MSF_REVISION_01);
            if (dec.ok()) {
                const moq_msf_track_supplement_t *s = dec->track(0).supplement;
                MOQ_CHECK(s && s->has_encryption_scheme && s->has_key_id &&
                          moq::msf::detail::sv(s->key_id) == "k");
            }
        }

        moq::msf::track k = t;
        k.keyframe_spacing = 15;
        cat.tracks = &k;
        auto ks = moq::msf::encode(cat);
        MOQ_CHECK(!ks.ok() && ks.error().code() == moq::errc::invalid);

        moq::msf::track g;
        g.name = "v"; g.packaging = "loc"; g.is_live = true;
        g.has_render_group = true; g.render_group = 1;
        g.depends_render_groups = {1};
        cat.tracks = &g;
        auto gs = moq::msf::encode(cat);
        MOQ_CHECK(!gs.ok() && gs.error().code() == moq::errc::invalid);

        moq::msf::track pm;
        pm.name = "v"; pm.packaging = "loc"; pm.is_live = true;
        pm.has_packaging_metadata = true; pm.packaging_metadata = "streamMapping=group";
        cat.tracks = &pm;
        auto ps = moq::msf::encode(cat);
        MOQ_CHECK(!ps.ok() && ps.error().code() == moq::errc::invalid);

        moq::msf::track rm;
        rm.name = "v";
        moq::msf::delta_op op;
        op.kind = moq::msf::delta_op_kind::remove;
        op.tracks = &rm; op.track_count = 1;
        moq::msf::catalog delta;
        delta.revision = MOQ_MSF_REVISION_02;
        delta.has_generated_at = true; delta.generated_at = 2;
        delta.delta_update = &op; delta.delta_update_count = 1;
        MOQ_CHECK(moq::msf::encode(delta).ok());
        delta.publish_tracks = &t; delta.publish_track_count = 1;
        auto pd = moq::msf::encode(delta);
        MOQ_CHECK(!pd.ok() && pd.error().code() == moq::errc::invalid);
    }

    // An MSF-02 "update" op authored, parsed and applied: the target is found
    // by its Full Track Name, a track without "namespace" inheriting the
    // catalog namespace in cfg.
    {
        auto base = moq::msf::parse(moq::bytes_view(
            R"({"version":"draft-02","tracks":[{"name":"video","packaging":"loc",)"
            R"("isLive":true,"bitrate":1000,"keyframeSpacing":30}]})"));
        MOQ_CHECK(base.ok());

        moq::msf::track up;
        up.has_parent_name = true; up.parent_name = "video";
        up.has_parent_namespace = true; up.parent_namespace = "live/cam1";
        up.has_bitrate = true; up.bitrate = 2000;
        up.keyframe_spacing = 60;
        up.has_key_id = true; up.key_id = "k2";
        moq::msf::delta_op op;
        op.kind = moq::msf::delta_op_kind::update;
        op.tracks = &up; op.track_count = 1;
        moq::msf::catalog dcat;
        dcat.revision = MOQ_MSF_REVISION_02;
        dcat.has_generated_at = true; dcat.generated_at = 7;
        dcat.delta_update = &op; dcat.delta_update_count = 1;
        auto denc = moq::msf::encode(dcat);
        MOQ_CHECK(denc.ok());
        if (base.ok() && denc.ok()) {
            auto delta = moq::msf::parse(moq::bytes_view(denc->data(), denc->size()));
            MOQ_CHECK(delta.ok());
            if (delta.ok()) {
                MOQ_CHECK(delta->revision() == MOQ_MSF_REVISION_02);
                MOQ_CHECK(delta->delta_op(0).kind() == moq::msf::delta_op_kind::update);
                auto ut = delta->delta_op(0).track(0);
                MOQ_CHECK(ut.parent_namespace.value_or("") == "live/cam1");
                MOQ_CHECK(ut.supplement && ut.supplement->keyframe_spacing == 60);

                // Without the catalog namespace the target is unknown.
                auto none = moq::msf::apply_delta(*base, *delta);
                MOQ_CHECK(!none.ok() && none.error().code() == moq::errc::protocol);

                moq::msf::validate_cfg cfg;
                cfg.catalog_namespace = moq::namespace_name{"live", "cam1"};
                auto eff = moq::msf::apply_delta(*base, *delta, cfg);
                MOQ_CHECK(eff.ok());
                if (eff.ok()) {
                    MOQ_CHECK(!eff->is_delta());
                    MOQ_CHECK(eff->revision() == MOQ_MSF_REVISION_02);
                    MOQ_CHECK(eff->track_count() == 1);
                    auto v = eff->track(0);
                    MOQ_CHECK(v.name == "video" && v.bitrate.value_or(0) == 2000);
                    MOQ_CHECK(v.supplement && v.supplement->keyframe_spacing == 60 &&
                              moq::msf::detail::sv(v.supplement->key_id) == "k2");
                }

                // A delta within its limit applies.
                moq::msf::validate_cfg tight = cfg;
                tight.max_delta_ops = 1;
                MOQ_CHECK(moq::msf::apply_delta(*base, *delta, tight).ok());

                // A delta is no base, and a base is no delta.
                auto swapped = moq::msf::apply_delta(*delta, *base, cfg);
                MOQ_CHECK(!swapped.ok() && swapped.error().code() == moq::errc::invalid);
            }
        }
    }

    // apply_delta judges a delta by the BASE's revision: an "update" op onto
    // an MSF-01 base is a protocol error; an MSF-01 delta still applies.
    {
        auto base = moq::msf::parse(moq::bytes_view(
            R"({"version":"draft-01","tracks":[{"name":"video","packaging":"loc",)"
            R"("isLive":true}]})"));
        auto update = moq::msf::parse(moq::bytes_view(
            R"({"generatedAt":1,"deltaUpdate":[{"op":"update","tracks":[)"
            R"({"parentName":"video","bitrate":5}]}]})"));
        auto add = moq::msf::parse(moq::bytes_view(
            R"({"deltaUpdate":[{"op":"add","tracks":[{"name":"audio",)"
            R"("packaging":"loc","isLive":true}]}]})"));
        MOQ_CHECK(base.ok() && update.ok() && add.ok());
        if (base.ok() && update.ok() && add.ok()) {
            auto u = moq::msf::apply_delta(*base, *update);
            MOQ_CHECK(!u.ok() && u.error().code() == moq::errc::protocol);
            auto a = moq::msf::apply_delta(*base, *add);
            MOQ_CHECK(a.ok());
            if (a.ok()) {
                MOQ_CHECK(a->revision() == MOQ_MSF_REVISION_01);
                MOQ_CHECK(a->track_count() == 2 && a->track(1).name == "audio");
            }
            moq::msf::validate_cfg limited;
            limited.max_effective_tracks = 1;
            auto over = moq::msf::apply_delta(*base, *add, limited);
            MOQ_CHECK(!over.ok() && over.error().code() == moq::errc::protocol);
        }
    }

    // check(): the first violation's reason as the value; the catalog
    // namespace decides which tracks share a Full Track Name.
    {
        auto ok = moq::msf::parse(moq::bytes_view(
            R"({"version":"draft-02","tracks":[)"
            R"({"name":"v","packaging":"loc","isLive":true,"renderGroup":1},)"
            R"({"name":"c","packaging":"loc","isLive":true,"depends":["v",1]}]})"));
        MOQ_CHECK(ok.ok());
        if (ok.ok()) {
            auto r = moq::msf::check(*ok);
            MOQ_CHECK(r.ok() && *r == MOQ_MSF_CATALOG_OK);
        }

        auto unresolved = moq::msf::parse(moq::bytes_view(
            R"({"version":"draft-02","tracks":[)"
            R"({"name":"c","packaging":"loc","isLive":true,"depends":["v"]}]})"));
        MOQ_CHECK(unresolved.ok());
        if (unresolved.ok()) {
            auto r = moq::msf::check(*unresolved);
            MOQ_CHECK(r.ok() && *r == MOQ_MSF_CATALOG_ERR_DEPENDS_UNRESOLVED);
        }

        auto twins = moq::msf::parse(moq::bytes_view(
            R"({"version":"draft-02","tracks":[)"
            R"({"name":"v","packaging":"loc","isLive":true},)"
            R"({"name":"v","namespace":"live/cam1","packaging":"loc","isLive":true}]})"));
        MOQ_CHECK(twins.ok());
        if (twins.ok()) {
            auto apart = moq::msf::check(*twins);
            MOQ_CHECK(apart.ok() && *apart == MOQ_MSF_CATALOG_OK);
            moq::msf::validate_cfg cfg;
            cfg.catalog_namespace = moq::namespace_name{"live", "cam1"};
            auto same = moq::msf::check(*twins, cfg);
            MOQ_CHECK(same.ok() && *same == MOQ_MSF_CATALOG_ERR_DUPLICATE_TRACK);
        }

        auto dup_pub = moq::msf::parse(moq::bytes_view(
            R"({"version":"draft-02","tracks":[],"publishTracks":[)"
            R"({"name":"log","packaging":"moqlog","isLive":true},)"
            R"({"name":"log","packaging":"moqlog","isLive":true}]})"));
        MOQ_CHECK(dup_pub.ok());
        if (dup_pub.ok()) {
            auto r = moq::msf::check(*dup_pub);
            MOQ_CHECK(r.ok() && *r == MOQ_MSF_CATALOG_ERR_DUPLICATE_PUBLISH_TRACK);
        }

        auto delta = moq::msf::parse(moq::bytes_view(
            R"({"deltaUpdate":[{"op":"remove","tracks":[{"name":"v"}]}]})"));
        MOQ_CHECK(delta.ok());
        if (delta.ok()) {
            auto r = moq::msf::check(*delta);
            MOQ_CHECK(!r.ok() && r.error().code() == moq::errc::invalid);
        }

        // A namespace longer than 32 parts is refused, not truncated.
        std::string_view parts[33];
        for (auto &p : parts) p = "x";
        moq::msf::validate_cfg overflow;
        overflow.catalog_namespace = moq::namespace_name(std::span<const std::string_view>(parts));
        MOQ_CHECK(overflow.catalog_namespace.overflowed());
        if (ok.ok()) {
            auto r = moq::msf::check(*ok, overflow);
            MOQ_CHECK(!r.ok() && r.error().code() == moq::errc::invalid);
        }
    }

    // namespace_split(): the MSF-02 5.2.2 fields, borrowed from the input.
    {
        const char text[] = "example.com/live/cam1";
        auto ns = moq::msf::namespace_split(moq::bytes_view(text));
        MOQ_CHECK(ns.ok());
        if (ns.ok()) {
            MOQ_CHECK(ns->count() == 3);
            MOQ_CHECK((*ns)[0] == "example.com" && (*ns)[1] == "live" && (*ns)[2] == "cam1");
            MOQ_CHECK((*ns)[2].data() == text + 17);
        }
        auto one = moq::msf::namespace_split(moq::bytes_view("solo"));
        MOQ_CHECK(one.ok() && one->count() == 1 && (*one)[0] == "solo");
        for (const char *bad : {"", "/a", "a/", "a//b"}) {
            auto r = moq::msf::namespace_split(moq::bytes_view(std::string_view(bad)));
            MOQ_CHECK(!r.ok() && r.error().code() == moq::errc::protocol);
        }
        std::string many;
        for (int i = 0; i < 32; i++) many += i ? "/f" : "f";
        auto max = moq::msf::namespace_split(moq::bytes_view(std::string_view(many)));
        MOQ_CHECK(max.ok() && max->count() == 32 && !max->overflowed());
        many += "/f";
        auto over = moq::msf::namespace_split(moq::bytes_view(std::string_view(many)));
        MOQ_CHECK(!over.ok() && over.error().code() == moq::errc::protocol);
        auto null_span = moq::msf::namespace_split(
            moq::bytes_view(static_cast<const uint8_t *>(nullptr), 1));
        MOQ_CHECK(!null_span.ok() && null_span.error().code() == moq::errc::invalid);
    }

    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
