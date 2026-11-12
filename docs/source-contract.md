# 门店与时段分页约定

GET /outlets 返回 value 数组，门店含 outletId、version、slotVersion、Slots 和可选的 Slots@odata.nextLink；根层 @odata.nextLink 独立控制下一页。GET /slots/A?cursor=x 返回 owner、version、value 和可选 @odata.nextLink。子页 version 对应 slotVersion，不是门店 version。slotId 仅在父门店内唯一；内联 Slots 是子集合第一页，无子续链表示该时段集合结束。

根记录和子记录允许相同内容重复，等版本不同内容是冲突。版本是不透明的观察标识，不比较字符串大小；门店版本变化要建立独立子代际，迟到旧子页不能拼入新代际。来源完整性必须同时证明根页和每个门店的子页，单个门店失败不取消其他已完整门店。

链接以产生它的响应地址解析，保留查询参数，不手工构造 skip。跨 origin、重复链接、缺业务键和空续页字符串均有界失败。配置提供 max_pages、concurrency、allowed_origin。回放允许先取子 A 或根 B；/loop 给出续链环。实际 HTTP 完成顺序不应影响身份归属或完整性账本。
